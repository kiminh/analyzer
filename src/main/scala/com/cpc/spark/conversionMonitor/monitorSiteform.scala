package com.cpc.spark.conversionMonitor

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object monitorSiteform {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val cnt3 = getDataV3(date, hour, spark)
    val cnt4 = getDataV3(date, hour, spark)
    val cnt5 = getDataV5(date, hour, spark)

    println(s"$cnt3, $cnt4, $cnt5")

  }

  def getDataV3(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    distinct searchid
         |from dl_cpc.ml_cvr_feature_v1
         |lateral view explode(cvr_list) b as a
         |where `date` = '$date' and `hour` = '$hour'
         |and access_channel="site"
         |and a in ('ctsite_form', 'site_form')
       """.stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest).count()

    result
  }

  def getDataV4(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    distinct searchid
         |from
         |    dl_cpc.dm_conversions_for_model
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    array_contains(conversion_target,'site_form')
       """.stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest).count()

    result
  }

  def getDataV5(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |select
         |    distinct searchid
         |from
         |    dl_cpc.cpc_conversion
         |where
         |    day='$date'
         |and
         |    `hour` = '$hour'
         |and
         |    array_contains(conversion_target,'site_form')
       """.stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest).count()

    result
  }




}



