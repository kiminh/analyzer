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
  * Created by
  */
object GetUserAntispam {

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetUserAntispam <day>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0)
//    val cal = Calendar.getInstance()
//    cal.add(Calendar.DATE, -dayBefore)
//    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val conf = ConfigFactory.load()

    val ctx = SparkSession.builder()
      .appName("cpc get user antispam [%s]".format(date))
      .enableHiveSupport()
      .getOrCreate()

    val hivesql = "select device from  rpt_ac.qukan_item_clk  where thedate=\"%s\" and rate > 0.7 and rate < 999 ".format(date)

    println("hive sql is [%s]".format(hivesql))

    val sum = ctx.sql(hivesql).rdd.mapPartitions {
      p =>
        var n1 = 0
        var n2 = 0
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        p.foreach {
          x =>
            n1 = n1 + 1
            var deviceid : String = x(0).toString()
            var user : UserProfile.Builder = null
            val key = deviceid + "_UPDATA"
            val buffer = redis.get[Array[Byte]](key).getOrElse(null)
            if (buffer == null) {
              user = UserProfile.newBuilder().setDevid(deviceid)
            }else {
              user = UserProfile.parseFrom(buffer).toBuilder
              if(user.getAntispam != 1){
                n2 = n2 + 1
              }
            }
            user.setAntispam(1)
            redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
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


