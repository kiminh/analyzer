package com.cpc.spark.small.tool

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import userprofile.Userprofile.{InterestItem, UserProfile}

/**
  * Created by wanli on 2017/7/27.
  */
object GetAddInterests {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val dayBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val pv = args(1).toInt

    println("small tool GetAddInterests run ... day %s , pv %d".format(day, pv))

    val conf = ConfigFactory.load()

    val ctx = SparkSession
      .builder()
      .appName("small tool GetAddInterests")
      .enableHiveSupport()
      .getOrCreate()

    val dctrpData = ctx.sql(
      """
        SELECT device,content_type,read_pv
        FROM rpt_qukan.rpt_qukan_device_content_type_read_pv
        WHERE thedate='%s' AND read_pv>%d AND content_type IS NOT NULL
      """.format(day, pv)).rdd

    val channal_tag = conf.getConfig("app_tag.v2.channal_tag")

    val sum = dctrpData
      .groupBy(_.getString(0))
      .mapPartitions {
        p =>
          val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
          var n = 0
          var n1 = 0
          var n2 = 0
          p.foreach {
            row =>
              n += 1
              val key = row._1 + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).getOrElse(null)
              if (buffer != null) {
                var tmpExit = false
                val user = UserProfile.parseFrom(buffer).toBuilder
                row._2.foreach {
                  x =>
                    val tagId = channal_tag.getInt(x.getString(1))
                    val score = Math.floor((x.getInt(2) / 2).toFloat).toInt
                    if (tagId > 0) {
                      val item = InterestItem.newBuilder().setTag(tagId).setScore(score)
                      val count = user.getInterestsCount
                      var hasExit = false

                      for (i <- 0 to count - 1) {
                        if (user.getInterests(i).getTag == item.getTag) {
                          hasExit = true
                        }
                      }

                      if (!hasExit) {
                        tmpExit = true
                        user.addInterests(item)
                      }
                    }
                }

                if (tmpExit) {
                  n1 += 1
                }

                redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)

              } else {

                var tmpExit = false
                var user = UserProfile.newBuilder().setDevid(row._1)

                row._2.foreach {
                  xx =>
                    val tagId = channal_tag.getInt(xx.getString(1))
                    val score = Math.floor((xx.getInt(2) / 2).toFloat).toInt
                    if (tagId > 0) {
                      tmpExit = true
                      val item = InterestItem.newBuilder().setTag(tagId).setScore(score)
                      user.addInterests(item)
                    }
                }

                if (tmpExit) {
                  n2 += 1
                  redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
                }

              }
          }
          Seq((0, n), (1, n1), (2, n2)).iterator
      }

    var n = 0
    var n1 = 0
    var n2 = 0
    sum.reduceByKey((a, b) => a + b)
      .take(3)
      .foreach {
        x =>
          if (x._1 == 0) {
            n += x._2
          } else if (x._1 == 1) {
            n1 += x._2
          } else if (x._1 == 2) {
            n2 += x._2
          }
      }

    println("small tool GetAddInterests n: %d ,n1: %d,n2: %d".format(n, n1,n2))
    ctx.stop()
  }
}
