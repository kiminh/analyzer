package com.cpc.spark.ml.stub

import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.log.parser.{ExtValue, LogParser, UnionLog}
import com.cpc.spark.ml.common.FeatureDict
import com.cpc.spark.ml.ctrmodel.v3.FeatureParser
import com.typesafe.config.ConfigFactory
import io.grpc.ManagedChannelBuilder
import mlserver.mlserver._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.IsotonicRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by zhaolei on 2017/12/14.
  */

object FeatureStub {

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
      .appName("stub lr server")
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

    val sql = "select * from dl_cpc.cpc_union_log where `date` = \"%s\" and media_appsid in (\"80000001\", \"80000002\") and adslot_type in (1,2)  and interests != \"\" and isclick = 1 and hour = \"%s\" and ext['antispam'].int_value = 0 limit %s".format(date, hour, args(3).toInt)

    println(sql)

    val logs = ctx.sql(sql).as[UnionLog].rdd
      .toLocalIterator.toSeq
    FeatureDict.loadData()

    val modelPath = args(2).trim
    val irPath = modelPath + "_ir"
    val model = LogisticRegressionModel.load(ctx.sparkContext, modelPath)
    val irmodel = IsotonicRegressionModel.load(ctx.sparkContext, irPath)

    println(model.numFeatures)

    println("start logs process")

    logs.foreach{
      x =>
        val (ad, m, ads, u, loc, n, d, t) = FeatureParser.unionLogToObject(x)
        val v = FeatureParser.getVector(FeatureDict.dict, ad, m, ads, u, loc, n, d, t)

        println(x.date, x.hour, x.timestamp)
        println(FeatureParser.vectorToSvm(v))

        val p = model.predict(MLUtils.appendBias(v))
        val p1 = irmodel.predict(p)

        val req = Request(
          version = "v2",
          ads = Seq(ad),
          media = Option(m),
          user = Option(u),
          loc = Option(loc),
          network = Option(n),
          device = Option(d),
          time = x.timestamp,
          cvrVersion = "cvr_v1",
          adSlot = Option(ads)
        )

        val blockingStub = PredictorGrpc.blockingStub(channel)

        val reply = blockingStub.predict(req)

        val ctr = x.ext.getOrElse("exp_ctr", ExtValue()).int_value / 1e6

        println(ad.ideaid, p1, reply.results(0).value, ctr)
        println("")
    }

  }

}

