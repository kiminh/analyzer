package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.qukan.parser.HdfsParser
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile
import com.redis.serialization.Parse.Implicits._

/**
  * Created by Roy on 2017/5/17.
  */
@deprecated
object GetUserProfileV2 {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserProfile <day_before>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

    val ctx = SparkSession.builder()
      .appName("cpc get user profile v2 [%s]".format(day))
      .getOrCreate()

    val path = "/tmp/device_member_age_sex/"

    val rdd = ctx.read.orc(path).rdd
      .map(x => HdfsParser.parseTextRowAgeSex(x))
      .filter(_ != null)

    val sum = rdd.mapPartitions {
      p =>
        var n1 = 0
        var n2 = 0
        var n3 = 0
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        p.foreach {
          x =>
            n1 = n1 + 1
            val key = x.devid + "_UPDATA"
            val buffer = redis.get[Array[Byte]](key).getOrElse(null)
            var user: UserProfile.Builder = null
            if (buffer == null) {
              user = UserProfile.newBuilder().setDevid(x.devid)
              n2 = n2 + 1
            } else {
              user = UserProfile.parseFrom(buffer).toBuilder
            }
            val u = user.build()
            if (u.getAge != x.age || u.getSex != x.sex) {
              n3 = n3 + 1
              user = user.setAge(x.age).setSex(x.sex)
              redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
            }
        }
        Seq((0, n1), (1, n2), (2, n3)).iterator
    }

    //统计新增数据
    var n1 = 0
    var n2 = 0
    var n3 = 0
    sum.reduceByKey((x, y) => x + y)
      .take(3)
      .foreach {
        x =>
          if (x._1 == 0) {
            n1 = x._2
          } else if (x._1 == 1) {
            n2 = x._2
          } else {
            n3 = x._2
          }
      }

    println("total: %d new: %d updated: %d".format(n1, n2, n3))
    ctx.stop()
  }
}

