package com.cpc.spark.oCPX.oCPC.calibration

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcCvrFactorBase {
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
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = args(4).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, expTag:$expTag, hourInt:$hourInt")

    val result = OcpcCvrFactorBaseMain(date, hour, version, expTag, hourInt, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_smooth_factor20190702a")
  }

  def OcpcCvrFactorBaseMain(date: String, hour: String, version: String, expTag: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseData = getBaseData(hourInt, date, hour, spark)

    // 计算结果
    val result = calculatePCOC(baseData, spark)

    val finalVersion = version + hourInt.toString
    val resultDF = result
      .select("identifier", "conversion_goal", "click", "cv", "pre_cvr", "total_price", "total_bid", "hour_cnt")
      .filter(s"cv > 0")

    resultDF
  }



  def calculatePCOC(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .groupBy("unitid", "conversion_goal")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        avg(col("exp_cvr")).alias("pre_cvr"),
        sum(col("price")).alias("total_price"),
        sum(col("bid")).alias("total_bid"),
        countDistinct(col("hour")).alias("hour_cnt")
      )
      .select("unitid", "conversion_goal", "click", "cv", "pre_cvr", "total_price", "total_bid", "hour_cnt")

    val result = data
        .selectExpr("cast(unitid as string) identifier", "conversion_goal", "click", "cv", "pre_cvr", "total_price", "total_bid", "hour_cnt")

    result
  }

}