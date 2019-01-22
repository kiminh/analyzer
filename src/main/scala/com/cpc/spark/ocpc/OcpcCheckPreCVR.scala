package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcCheckPreCVR {
  def main(args: Array[String]): Unit = {
    /*
    抽取推荐cpa中的unitid，比较昨天和今天的平均预测cvr差距
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString

    // 抽取推荐cpa的unitid
    val data = getSuggestUnitid(date, hour, spark)
    // 根据slim_unionlog计算平均预测cvr
    val result = cmpPreCvr(data, date, hour, spark)
  }

  def cmpPreCvr(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  exp_cvr,
         |  dt as date
         |FROM
         |  dl_cpc.slim_unionlog
         |WHERE
         |  (`dt`='$date1' or `dt`='$date')
         |AND isclick=1
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)

    rawData
  }

  def getSuggestUnitid(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |  unitid
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |  `date`='$date1'
         |AND
         |  `hour`='23'
         |AND
         |  version='qtt_demo'
         |AND
         |  is_recommend=1
         |AND
         |  industry in ('feedapp', 'elds')
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).distinct()

    data
  }



}

