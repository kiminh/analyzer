package com.cpc.spark.oCPX.oCPC.calibration_by_tag.pred_v1

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag1 = args(3).toString
    val expTag2 = args(4).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag1:$expTag1, expTag2:$expTag2")

    // 读取baseline校准数据
    val baselineData = getBaselineData(date, hour, version, expTag2, spark)

    // 读取pcoc预估模型校准数据

    // 读取筛选词表

    // 推送至校准数据表

  }

  def getExpTags(expTag: String, spark: SparkSession) = {
    var editExpTag = expTag
    if (expTag == "base") {
      editExpTag = ""
    }
    val qtt = editExpTag + "Qtt"
    val midu = editExpTag + "MiDu"
    val hottopic = editExpTag + "HT66"
    val others = editExpTag + "Other"
    val result = s"exp_tag in ('$qtt', '$midu', '$hottopic', '$others')"
    result
  }

  def getBaselineData(date: String, hour: String, version: String, expTag: String, spark: SparkSession) = {
    val expTagSelection = getExpTags(expTag, spark)
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_pb_data_hourly_exp
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  $expTagSelection
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    data.show(10)
    data
  }

}


