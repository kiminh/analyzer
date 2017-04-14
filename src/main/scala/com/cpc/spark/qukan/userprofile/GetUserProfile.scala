package com.cpc.spark.qukan.userprofile

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.qukan.parser.HdfsParser
import com.redis.RedisClient
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Roy on 2017/4/14.
  */
object GetUserProfile {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CPC get qukan user profile")
    val sc = new SparkContext(conf)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -2)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hdfs = "/warehouse/rpt_qukan.db/device_member_coin/thedate=%s/*".format(yesterday)
    val ctx = sc.textFile(hdfs)
    val redis = new RedisClient("10.9.125.57", 6379)

    val data = ctx.map {
      case row =>
        HdfsParser.parseTextRow(row)
    }.filter {
      case (ok, devid, profile) =>
        ok
    }.map {
      case (ok, devid, profile) =>
        (devid, profile)
    }

    data.take(1).foreach {
      row =>

        println("---", row)
    }

    sc.stop()
  }
}
