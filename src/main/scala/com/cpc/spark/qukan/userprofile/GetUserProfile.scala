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
    cal.add(Calendar.DATE, -2)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val redis = new RedisClient("10.9.125.57", 6379)

    for (n <- 0 to 14) {
      var path: String = ""
      if (n < 10) {
        path = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s/00000%d_0".format(day, n)
      } else {
        path = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s/0000%d_0".format(day, n)
      }

      val ctx = sc.textFile(path)
      val data = ctx.map {
        case row =>
          HdfsParser.parseTextRow(row)
      }.filter {
        case (ok, devid, age, sex, coin) =>
          ok
      }.map {
        case (ok, devid, age, sex, coin) =>
          (devid, (age, sex, coin))
      }.reduceByKey {
        case (x, y) => y
      }.map {
        case (devid, (age, sex, coin)) =>
          (devid, age, sex, coin)
      }

      data.collect().foreach {
        row =>
          val devid = row._1
          val profile = Userprofile
            .UserProfile
            .newBuilder()
            .setAge(row._2)
            .setSex(row._3)
            .setCoin(row._4)
            .build()

          redis.set(devid, profile.toByteArray)
      }
    }
    sc.stop()
  }
}
