package com.cpc.spark.ml.stub


import com.cpc.spark.ml.ctrmodel.v1.LRTrain
import com.typesafe.config.ConfigFactory
import io.grpc.ManagedChannelBuilder
import mlserver.mlserver._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.IsotonicRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import userprofile.Userprofile.{APPPackage, UserProfile}

/**
  * Created by zhaolei on 2017/12/14.
  */

object FeatureStubNew {

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


    val conf = ConfigFactory.load()
    val channel = ManagedChannelBuilder
      .forAddress(args(0), args(1).toInt)
      .usePlaintext(true)
      .build


    val date = "2017-12-26"
    val hour = "13"

    val sql = "select searchid,isclick as label,sex,age,os,isp,network,city,media_appsid,ext['phone_level'].int_value as phone_level,`timestamp`,adtype,planid,unitid,ideaid,ext['adclass'].int_value as adclass,adslotid,adslot_type,ext['pagenum'].int_value as pagenum,ext['bookid'].string_value as bookid,ext['user_req_ad_num'].int_value as user_req_ad_num,ext[ 'user_req_num'].int_value as user_req_num, uid from dl_cpc.cpc_union_log where `date` = \"%s\" and media_appsid in (\"80000001\", \"80000002\") and adslot_type in (1,2)  and interests != \"\" and isclick = 1 and hour = \"%s\" and ext['antispam'].int_value = 0 limit %s".format(date, hour, args(4).toInt)

    println(sql)

    val logs = ctx.sql(sql).rdd

    val modelPath = args(2).trim
    val irPath = args(3).trim
    val model = LogisticRegressionModel.load(ctx.sparkContext, modelPath)
    val irmodel = IsotonicRegressionModel.load(ctx.sparkContext, irPath)

    println(model.numFeatures)

    println("start logs process")

    logs.toLocalIterator.foreach{
      x =>

        val v = LRTrain.getVectorParser3(x)
        println(v)
        val p = model.predict(v)
        val p1 = irmodel.predict(p)

        var seq = Seq[String]()
        val uid = x.getAs[String]("uid") + "_UPDATA"

        val conf = ConfigFactory.load()
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        val buffer = redis.get[Array[Byte]](uid).getOrElse(null)
        if (buffer != null) {
          val installpkg = UserProfile.parseFrom(buffer).getInstallpkgList
          for( i <- 0 until installpkg.size){
            seq = seq :+ installpkg.get(i).getPackagename
          }
        }

        val (ad, m, slot, u, loc, n, d, t) = LRTrain.unionLogToObject(x,seq)

        val req = Request(
          version = "v3",
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

        //val ctr = x.ext.getOrElse("exp_ctr", ExtValue()).int_value / 1e6

        println(ad.ideaid, p1, reply.results(0).value)
        println("")
    }

  }

}

