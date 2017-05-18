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
  * Created by Roy on 2017/4/14.
  */
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
    val day = HdfsParser.dateFormat.format(cal.getTime)
    val conf = ConfigFactory.load()

    val ctx = SparkSession.builder()
      .appName("cpc get user profile [%s]".format(day))
      .getOrCreate()

    val profilePath = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s".format(day)
    /*
    val urdd = ctx.read.text(profilePath).rdd
      .map(x => HdfsParser.parseTextRow(x.getString(0)))
      .filter(x => x != null && x.devid.length > 0)
      */

    val urdd = ctx.sql(
      """
        |select member_id,type from algo_lechuan.user_preferred_type
      """.stripMargin).rdd

    val sum = urdd.mapPartitions {
      p =>
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        var n1 = 1
        var n2 = 2
        /*
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
            user = user.setAge(x.age)
              .setSex(x.sex)
              .setCoin(x.coin)
            redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
        }
        */
        Seq((0, n2), (1, n1)).iterator
    }



    //统计新增数据
    var total = 0
    var n = 0
    sum.reduceByKey((x, y) => x + y)
      .take(2)
      .foreach {
        x =>
          if (x._1 == 0) {
            total = x._2
          } else {
            n = x._2
          }
      }

    println("total: %d new: %d".format(total, n))
    ctx.stop()
  }
}


