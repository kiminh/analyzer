package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.qukan.parser.HdfsParser
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.UserProfile
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._

/**
  * Created by Roy on 2017/4/14.
  */
@deprecated
object GetUserProfile {

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

    val ctx = SparkSession.builder()
      .appName("cpc get user profile [%s]".format(day))
      .getOrCreate()


    val profilePath = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s".format(day)
    val urdd = ctx.read.text(profilePath).rdd
      .map(x => HdfsParser.parseTextRow(x.getString(0)))
      .filter(x => x != null && x.devid.length > 0)

    val sum = urdd.mapPartitions {
      p =>
        var n1 = 0
        var n2 = 0
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        p.foreach {
          x =>
            n1 = n1 + 1
            val key = x.devid + "_UPDATA"
            val buffer = redis.get[Array[Byte]](key).getOrElse(null)
            if (buffer != null) {
              val u = UserProfile.parseFrom(buffer)
              if (u.getCoin != x.coin ) {
                n2 = n2 + 1
                val user = u.toBuilder.setCoin(x.coin)
                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
              }
            }
        }
        Seq((0, n1), (1, n2)).iterator
    }

    //统计新增数据
    var n1 = 0
    var n2 = 0
    sum.reduceByKey((x, y) => x + y)
      .take(3)
      .foreach {
        x =>
          if (x._1 == 0) {
            n1 = x._2
          } else {
            n2 = x._2
          }
      }

    println("total: %d updated: %d".format(n1, n2))
    ctx.stop()
  }
}


