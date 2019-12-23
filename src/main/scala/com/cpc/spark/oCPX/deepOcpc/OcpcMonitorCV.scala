package com.cpc.spark.oCPX.deepOcpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import com.cpc.spark.oCPX.deepOcpc.DeepOcpcTools._
import com.cpc.spark.ocpcV3.utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcMonitorCV{
  def main(args: Array[String]): Unit = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt:$hourInt")

    val result = OcpcMonitorCVmain(date, spark)

    val resultDF = result
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cv_data20191223")

  }

  def OcpcMonitorCVmain(date: String, spark: SparkSession) = {
    val clickData = getClickData(date, spark)
    val cvData = getConvData(date, spark)

    val data = clickData
      .join(cvData, Seq("unitid", "deep_conversion_goal"), "left_outer")
      .na.fill(0, Seq("cv"))
      .filter(s"cost >= 500")

    data
  }

  def getClickData(date: String, spark: SparkSession) = {
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
         |  unitid,
         |  deep_conversion_goal,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then price else 0 end) * 0.01 as cost
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  date = '$date1'
         |AND
         |  deep_ocpc_step = 2
         |GROUP BY unitid, deep_conversion_goal
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getConvData(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  deep_conversion_goal,
         |  count(distinct searchid) as cv
         |FROM
         |  dl_cpc.ocpc_label_deep_cvr_hourly
         |WHERE
         |  date = '$date'
         |GROUP BY unitid, deep_conversion_goal
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }


}