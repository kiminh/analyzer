package com.cpc.spark.oCPX.deepOcpc.calibration_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfDetermineMedia, udfMediaName, udfSetExpTag}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb {
  /*
  整合整个实验版本下的不同深度转化目标的数据
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = args(4).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt:$hourInt")

    // 计算计费比系数、后验激活转化率、先验点击次留率
    val result = getData(date, hour, version, expTag, spark)

    val resultDF = result
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")


    resultDF
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_deep_pb_data_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_pb_data_hourly")


  }

  def getData(date: String, hour: String, version: String, expTag: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_deep_pb_data_hourly_exp
         |WHERE
         |  date = '$date'
         |AND
         |  hour = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  exp_tag = '$expTag'
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
  }

}
