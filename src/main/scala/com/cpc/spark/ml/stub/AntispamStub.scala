package com.cpc.spark.ml.stub

import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.cpc.spark.ml.antimodel.v1.FeatureParser
import com.cpc.spark.ml.common.FeatureDict
import com.typesafe.config.ConfigFactory
import io.grpc.ManagedChannelBuilder
import mlserver.mlserver._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 2017/5/11.
  */
object AntispamStub {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: stub server <ip> <port> <modelpath>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .appName("antispam stub lr server")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val conf = ConfigFactory.load()
    val channel = ManagedChannelBuilder
      .forAddress(args(0), args(1).toInt)
      .usePlaintext(true)
      .build
    val date = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime)
    val hour = new SimpleDateFormat("HH").format(new Date().getTime - 7200L * 1000L)
    val sql = ("select * from dl_cpc.cpc_union_log where `date` = \"%s\" and isclick = 1 and hour = \"%s\" and " +
      "ext['antispam_predict'].float_value > 0 limit %s")
      .format("2017-11-01", "10", "100")
    println("sql:" + sql)
    val logs = ctx.sql(sql).as[UnionLog].rdd
      .toLocalIterator.toSeq
    FeatureDict.loadData()
    val modelPath = args(2)
    val model = LogisticRegressionModel.load(ctx.sparkContext, modelPath)
    println(model.numFeatures)
    logs.foreach{
      x =>
        val (ad, m, u, loc, n, d,ex , t) = FeatureParser.unionLogToObject(x)
        val v = FeatureParser.getVector(x,FeatureDict.dict)
        println(x.date, x.hour, x.timestamp)
        println(FeatureParser.vectorToSvm(v))
        val p = model.predict(MLUtils.appendBias(v))
        val req = Request(
          version = "v1",
          media = Option(m),
          user = Option(u),
          loc = Option(loc),
          network = Option(n),
          device = Option(d),
          extra = Option(ex),
          time = x.timestamp,
          searchTimestamp = x.click_timestamp,
          antispamVersion = "antispam_v1"
        )

        val blockingStub = PredictorGrpc.blockingStub(channel)
        val reply = blockingStub.predict(req)
        val ctr = x.ext.getOrElse("antispam_predict", ExtValue()).float_value
       println(p, reply.results(0).antispam, ctr)
    }

  }

}
