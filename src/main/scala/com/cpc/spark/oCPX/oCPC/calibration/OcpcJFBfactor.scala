package com.cpc.spark.oCPX.oCPC.calibration

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcJFBfactor {
  def main(args: Array[String]): Unit = {
    /*
    计算计费比
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

    val result = OcpcJFBfactorMain(date, hour, version, expTag, hourInt, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_jfb_factor20190723a")
  }

  def OcpcJFBfactorMain(date: String, hour: String, version: String, expTag: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseData = getBaseData(hourInt, date, hour, spark)

    // 计算结果
    val result = calculateJFB(baseData, spark)

    val resultDF = result
      .select("unitid", "conversion_goal", "media", "click", "total_price", "total_bid")
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
      .withColumn("version", lit(version))


    resultDF
  }



  def calculateJFB(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("unitid", "conversion_goal", "media")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("price")).alias("total_price"),
        sum(col("bid")).alias("total_bid")
      )
      .select("unitid", "conversion_goal", "media", "click", "total_price", "total_bid")

    val result = data
        .selectExpr("unitid", "conversion_goal", "media", "click", "total_price", "total_bid")

    result
  }

}