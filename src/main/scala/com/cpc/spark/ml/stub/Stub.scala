package com.cpc.spark.ml.stub

import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.log.parser.{ExtValue, LogParser, UnionLog}
import com.cpc.spark.ml.common.FeatureDict
import com.cpc.spark.ml.ctrmodel.gbdt.Train
import com.cpc.spark.ml.ctrmodel.hourly.LRTrain
import com.cpc.spark.ml.ctrmodel.v1.FeatureParser
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import io.grpc.ManagedChannelBuilder
import ml.dmlc.xgboost4j.scala.spark.XGBoostModel
import mlserver.mlserver._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.IsotonicRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile

/**
  * Created by roydong on 2017/5/11.
  */
object Stub {

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

    val sql =
      """
        |select searchid,isclick as label,sex,age,os,isp,network,city,media_appsid,
        |ext['phone_level'].int_value as phone_level,`timestamp`,
        |adtype,planid,unitid,ideaid,ext['adclass'].int_value as adclass,
        |adslotid,adslot_type,ext['pagenum'].int_value as pagenum,ext['bookid'].string_value as bookid,
        |ext['user_req_ad_num'].int_value as user_req_ad_num,ext[ 'user_req_num'].int_value as user_req_num, uid
        |from dl_cpc.cpc_union_log
        |where `date` = "%s" and media_appsid in ("80000001", "80000002") and adslot_type in (1,2)
        | and isclick = 1 and ext['antispam'].int_value = 0 limit %d"""
        .stripMargin.format(args(2), args(3).toInt)

    println(sql)
    val logs = ctx.sql(sql).cache()

    val model = XGBoostModel.load("/user/cpc/xgboost/ctr_v1")

    val vecs = logs.rdd.map { r => Train.getVectorParser2(r).toDense }

    vecs.toLocalIterator.foreach{
      v =>
        println(v.values.mkString(" "))
    }

    model.predict(vecs, 0.0001f)
      .toLocalIterator
      .foreach {
        x =>
          println(x.mkString(" "))
      }

    logs.rdd.toLocalIterator.foreach{
      x =>
        var seq = Seq[String]()
        val uid = x.getAs[String]("uid") + "_UPDATA"
        println(uid)

        val conf = ConfigFactory.load()
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        val buffer = redis.get[Array[Byte]](uid).getOrElse(null)
        if (buffer != null) {
          val installpkg = UserProfile.parseFrom(buffer).getInstallpkgList
          for( i <- 0 until installpkg.size){
            val name = installpkg.get(i).getPackagename
            seq = seq :+ name
          }
        }

        val (ad, m, slot, u, loc, n, d, t) = LRTrain.unionLogToObject(x,seq)

        val req = Request(
          version = "v1",
          ads = Seq(ad),
          media = Option(m),
          user = Option(u),
          loc = Option(loc),
          network = Option(n),
          device = Option(d),
          time = x.getAs[Int]("timestamp"),
          cvrVersion = "cvr_v1",
          adSlot = Option(slot)
        )

        val blockingStub = PredictorGrpc.blockingStub(channel)
        val reply = blockingStub.predict(req)

        println("ideaid=%d scala=%.8f ml=%.8f".format(ad.ideaid, 0d, reply.results(0).value))
        println("")
    }

  }

}
