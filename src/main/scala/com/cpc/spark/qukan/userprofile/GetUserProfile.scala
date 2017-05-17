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
           |Usage: GetUserProfile <day_before> <v>
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
      .appName("cpc get user profile [%s]".format(day))
      .enableHiveSupport()
      .getOrCreate()

    val profilePath = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s".format(day)
    var total = 0
    var n = 0
    ctx.read.text(profilePath).rdd
      .map(x => HdfsParser.parseTextRow(x.getString(0)))
      .filter(x => x != null && x.devid.length > 0)
      .toLocalIterator
      .foreach {
        x =>
          val key = x.devid + "_UPDATA"
          val buffer = redis.get[Array[Byte]](key).getOrElse(null)
          var user: UserProfile.Builder = null
          if (buffer == null) {
            user = UserProfile.newBuilder().setDevid(x.devid)
            n = n + 1
          } else {
            user = UserProfile.parseFrom(buffer).toBuilder
          }
          user = user.setAge(x.age)
            .setSex(x.sex)
            .setCoin(x.coin)
          redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
          total = total + 1
      }

    println("total: %d new: %d".format(total, n))
    ctx.stop()
  }
}

