package com.cpc.spark.conversionMonitor.cvrWarning

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object cvrCmpDaily {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString

    val dataToday = getData(date, spark)
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val dataYesterday = getData(date1, spark)

    // 数据对比
    val cmpResult = cmpData(dataToday, dataYesterday, spark)
    val result = cmpResult
      .withColumn("date", lit(date))
      .select("cvr_yesterday", "cvr_today", "cvr_diff", "conversion_goal", "version", "date", "hour")

    result
      .repartition(1)
      .write.mode("overwrite").insertInto("test.cv_goal_cvr_cmp_hourly")


  }

  def cmpData(dataToday: DataFrame, dataYesterday: DataFrame, spark: SparkSession) = {
    val data0 = dataToday
      .withColumn("cvr_today", col("cvr"))
      .select("conversion_goal", "cvr_today", "hour", "version")

    val data1 = dataYesterday
      .withColumn("cvr_yesterday", col("cvr"))
      .select("conversion_goal", "cvr_yesterday", "hour", "version")

    val data = data1
      .join(data0, Seq("conversion_goal", "hour", "version"), "outer")
      .withColumn("cvr_diff", (col("cvr_today") - col("cvr_yesterday")) / col("cvr_yesterday"))
      .na.fill(1, Seq("cvr_diff"))
      .withColumn("cvr_diff", abs(col("cvr_diff")))
      .select("hour", "cvr_yesterday", "cvr_today", "cvr_diff", "conversion_goal", "version")


    data
  }

  def getData(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  test.cv_goal_cvr_hourly
         |WHERE
         |  date = '$date'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
  }



}