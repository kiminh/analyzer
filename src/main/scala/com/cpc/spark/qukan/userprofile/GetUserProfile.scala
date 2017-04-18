package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.qukan.parser.HdfsParser
import com.redis.RedisClient
import org.apache.spark.{SparkConf, SparkContext}
import userprofile.Userprofile

/**
  * Created by Roy on 2017/4/14.
  */
object GetUserProfile {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("cpc get qukan user profile")
    val sc = new SparkContext(conf)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val redis = new RedisClient("10.9.125.57", 6379)

    for (n <- 0 to 14) {
      val path = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s/%06d*".format(day, n)
      val ctx = sc.textFile(path)
      val result = ctx.map(row => HdfsParser.parseTextRow(row))
        .filter(x => x.ok)
        .map(x => (x.devid, x))
        .reduceByKey((x, y) => y)
        .map(x => x._2)

      result.collect().foreach {
        row =>
          val devid = row.devid
          val profile = Userprofile
            .UserProfile
            .newBuilder()
            .setAge(row.age)
            .setSex(row.sex)
            .setCoin(row.coin)
            .build()
          redis.set(devid, profile.toByteArray)
      }
    }

    sc.stop()
  }
}
