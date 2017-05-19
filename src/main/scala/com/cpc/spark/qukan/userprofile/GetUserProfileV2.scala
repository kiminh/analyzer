package com.cpc.spark.qukan.userprofile

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
object GetUserProfileV2 {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserProfile <path>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = HdfsParser.dateFormat.format(cal.getTime)
    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))

    val ctx = SparkSession.builder()
      .appName("cpc get user profile v2 [%s]".format(day))
      .getOrCreate()

    val path = "/tmp/device_member_age_sex/"

    val rdd = ctx.read.orc(path).rdd
      .map(x => HdfsParser.parseTextRowAgeSex(x))
      .filter(_ != null)
      .cache()


    var n = rdd.count()
    println("c", n)
    println("-------age:")
    rdd.map(x => (x.age, 1))
      .reduceByKey((x, y) => x + y)
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          println("age %d: %d %.3f".format(x._1, x._2, x._2.toFloat / n.toFloat))
      }

    println("-------sex:")
    rdd.map(x => (x.sex, 1))
      .reduceByKey((x, y) => x + y)
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          println("sex %d: %d %.3f".format(x._1, x._2, x._2.toFloat / n.toFloat))
      }

    println("------------------")
    println("-----old----------")
    val profilePath = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s".format(day)
    val rdd1 = ctx.read.text(profilePath).rdd
      .map(x => HdfsParser.parseTextRow(x.getString(0)))
      .filter(_ != null)
      .cache()

    n = rdd1.count()
    println("c", n)
    println("-------age:")
    rdd1.map(x => (x.age, 1))
      .reduceByKey((x, y) => x + y)
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          println("age %d: %d %.3f".format(x._1, x._2, x._2.toFloat / n.toFloat))
      }

    println("-------sex:")
    rdd1.map(x => (x.sex, 1))
      .reduceByKey((x, y) => x + y)
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          println("sex %d: %d %.3f".format(x._1, x._2, x._2.toFloat / n.toFloat))
      }

    /*
    rdd.toLocalIterator
      .foreach {
        x =>
          /*
          val key = x.devid + "_UPDATA"
          val buffer = redis.get[Array[Byte]](key).getOrElse(null)
          var user: UserProfile.Builder = null
          if (buffer == null) {
            user = UserProfile.newBuilder().setDevid(x.devid)
            n = n + 1
          } else {
            user = UserProfile.parseFrom(buffer).toBuilder
          }
          user = user.setAge(x.age).setSex(x.sex)
          redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
          total = total + 1
          */

          age(x.age) = age(x.age) + 1
          sex(x.sex) = sex(x.sex) + 1
          total = total + 1
      }
      */

    ctx.stop()
  }
}

