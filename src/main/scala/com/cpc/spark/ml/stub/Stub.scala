package com.cpc.spark.ml.stub

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.log.parser.{ExtValue, LogParser, UnionLog}
import com.cpc.spark.ml.common.FeatureDict
import com.cpc.spark.ml.ctrmodel.gbdt.Train
import com.cpc.spark.ml.ctrmodel.hourly.LRTrain
import com.cpc.spark.ml.ctrmodel.v1.FeatureParser
import com.cpc.spark.ml.xgboost.SaveSampleSvm
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import io.grpc.ManagedChannelBuilder
import ml.dmlc.xgboost4j.java.DMatrix
import ml.dmlc.xgboost4j.scala.spark.XGBoostModel
import ml.dmlc.xgboost4j.scala.XGBoost
import ml.dmlc.xgboost4j.scala.DMatrix
import mlserver.mlserver.PredictorGrpc.PredictorStub
import mlserver.mlserver._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.IsotonicRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.json4s.native.Printer
import userprofile.Userprofile.UserProfile
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/**
  * Created by roydong on 2017/5/11.
  */
object Stub {

  var spark: SparkSession = null;

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
    spark = ctx;

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
    //val logs = ctx.sql(sql).cache()
    //logs.toDF().write.mode(SaveMode.Overwrite).parquet("/user/cpc/xgboost/ulog")
    var qtt = ctx.read.parquet("/user/cpc/xgboost/ulog")
    //val model = XGBoostModel.load("/home/cpc/anal/xgboost_model/ctr-tmp.gbm")
    //val booster = XGBoost.loadModel("/home/cpc/anal/xgboost_model/ctr-tmp.gbm")
    val irmodel = IsotonicRegressionModel.load(ctx.sparkContext, "/user/cpc/xgboost/xgbmodeldata/ctr_2018-03-13-07-00.ir")


    qtt = freqTransform(qtt, "city", "citydx")
    qtt = freqTransform(qtt, "adslotid", "adslotidx")
    qtt = freqTransform(qtt, "adclass", "adclassdx")
    qtt = freqTransform(qtt, "planid", "plandx")
    qtt = freqTransform(qtt, "unitid", "unitdx")
    qtt = freqTransform(qtt, "ideaid", "ideadx")

    val vecs = qtt.rdd
      .map {
        r =>
          SaveSampleSvm.getVectorParser2(r).toDense
      }

    val w = new FileWriter("./test_vec.svm")
    vecs.toLocalIterator.foreach {
      v =>
        println(v.values.mkString(" "))
        var svm = "1"
        v.foreachActive {
          (i, v) =>
            svm = svm + " %d:%f".format(i + 1, v)
        }
        svm
        println(svm)
        w.write(svm + "\n")
    }
    w.close()


    /*
    model.predict(vecs, 0.0001f)
      .toLocalIterator
      .foreach {
        x =>
          val v = irmodel.predict(x(0))
          println(x(0), v)
      }
      */

    for (i <- 1 to 100000) {
      Thread.sleep(10000)
      qtt.rdd.toLocalIterator.foreach{
        x =>
          var seq = Seq[String]()
          val uid = x.getAs[String]("uid") + "_UPDATA"
          println(uid)

          /*
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
          */

          /*val (ad, m, slot, u, loc, n, d, t) = LRTrain.unionLogToObject(x,seq)
          var ads = Seq[AdInfo]()
          for (i <- 1 to 1) {
            ads = ads :+ ad
          }

          val req = Request(
            version = "v1",
            ads = ads,
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

          println("ideaid=%d scala=%.8f ml=%.8f".format(ad.ideaid, 0d, reply.results(0).value))*/
          println("")

      }
    }
  }

  def freqTransform(src: DataFrame, in: String, out: String): DataFrame = {
    val dict = spark.read.parquet("/user/cpc/xgboost_dict/%s-%s".format(in, out))
    src.join(dict, Seq(in), "left_outer")
  }

}
