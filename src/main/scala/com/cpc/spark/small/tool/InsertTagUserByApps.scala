package com.cpc.spark.small.tool

import com.google.gson.JsonParser
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{InterestItem, UserProfile}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import com.redis.serialization.Parse.Implicits._

/**
  * Created by wanli on 2017/12/21.
  */
object InsertTagUserByApps {
  def main(args: Array[String]): Unit = {

    val argDay = args(0).toString

    val ctx = SparkSession
      .builder()
      .appName("InsertTagUserByApps day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    val conf = ConfigFactory.load("interest")
    val pconf = conf.getConfigList("user_tag_by_apps.qipai.contains")
    var pkgs = Seq[String]()
    for (i <- 0 until pconf.size()) {
      val c = pconf.get(i)
      pkgs = pkgs :+ c.getString("pkg")
    }

    val appData = ctx.sql(
      """
        |SELECT device,type,data
        |from gobblin.qukan_extend_report_p qerp
        |WHERE day="%s"
      """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val device = x.getString(0)
          val ttype = x.getInt(1)
          val data = if (x.getString(2).startsWith("[")) x.getString(2) else ""
          (device, ttype, data)
      }
      .filter(_._3.length > 0)
      .map {
        x =>
          val ttype = x._2
          val parser = new JsonParser()
          val jsonArray = parser.parse(x._3).getAsJsonArray
          val ans = new ArrayBuffer[(String)]()

          for (user <- jsonArray) {
            if (!user.isJsonNull) {
              var appName = ""
              if (ttype == 1) {
                appName = user.getAsJsonObject.get("packageName").toString
              } else if (ttype == 2) {
                appName = user.getAsString
              }

              if (appName.length > 0) {
                ans += appName
              }

            }

          }

          (x._1, (ans.filter(pkgs.contains(_))))
      }
      .filter(_._2.length > 0)
      .reduceByKey {
        (a, b) =>
          a ++= b
          (a)
      }
      .map(_._1)
      .cache()

    println("appData", appData.count())
    val sum = tagUser(appData, conf.getInt("user_tag_by_apps.qipai.tag_id"))
    println("qipai", sum)

  }

  def tagUser(uids: RDD[String], tag: Int): (Int, Int) = {
    val conf = ConfigFactory.load()
    val sum = uids
      .mapPartitions {
        p =>
          var n = 0
          var n1 = 0
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          p.foreach {
            uid =>
              val key = uid + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).orNull
              if (buffer != null) {
                val user = UserProfile.parseFrom(buffer).toBuilder
                val in = InterestItem.newBuilder()
                  .setTag(tag)
                  .setScore(100)
                var has = false
                for (i <- 0 until user.getInterestedWordsCount) {
                  val w = user.getInterestedWords(i)
                  if (w.getTag == in.getTag) {
                    if (!has) {
                      user.setInterestedWords(i, in)
                      has = true
                    } else {
                      user.removeInterestedWords(i)
                    }
                  }
                }
                if (!has) {
                  user.addInterestedWords(in)
                  n1 = n1 + 1
                }
                n = n + 1
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
          }
          Seq((n, n1)).iterator
      }
    sum.reduce((x, y) => (x._1 + y._1, x._2 + y._2))
  }
}
