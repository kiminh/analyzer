package com.cpc.spark.ml.video_model

import java.util.Properties

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object resource {
  Logger.getRootLogger.setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("video resource").enableHiveSupport().getOrCreate()
    val jdbcProp = new Properties()
    val jdbcUrl = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    jdbcProp.put("user", "adv_live_read")
    jdbcProp.put("password", "seJzIPUc7xU")
    jdbcProp.put("driver", "com.mysql.jdbc.Driver")
    val adv_result=
      s"""
         |(SELECT trim(tb.id) as ideaid, trim(ta.file_md5) as resource FROM adv.resource ta
         |join adv.idea tb on ta.id=tb.video_id group by trim(tb.id), trim(ta.file_md5)) temp
      """.stripMargin
    spark.read.jdbc(jdbcUrl, adv_result, jdbcProp).repartition(1000).createOrReplaceTempView("adv_result")
    spark.sql(
      s"""
         |insert overwrite table dl_cpc.resource partition (model="video")
         |select ideaid, resource from adv_result
       """.stripMargin)

  }
}
