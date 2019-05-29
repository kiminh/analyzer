package com.cpc.spark.OcpcProtoType.model_qtt

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.model_v3.OcpcSmoothFactor._


object OcpcSmoothFactor{
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
    val media = args(2).toString
    val hourInt = args(3).toInt
    val cvrType = args(4).toString
    val version = args(5).toString
    val minCV = 20
    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, hourInt:$hourInt, cvrType:$cvrType, minCV:$minCV")

    val baseData = getBaseData(media, cvrType, hourInt, date, hour, spark)

    // 计算结果
    val result = calculateSmoothV2(baseData, minCV, spark)

    var conversionGoal = 1
    if (cvrType == "cvr1") {
      conversionGoal = 1
    } else if (cvrType == "cvr2") {
      conversionGoal = 2
    } else {
      conversionGoal = 3
    }
    val resultDF = result
        .select("identifier", "pcoc", "jfb", "post_cvr")
        .withColumn("conversion_goal", lit(conversionGoal))
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit(version))

    resultDF.show()

    resultDF
//      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_pcoc_jfb_hourly")
      .repartition(5).write.mode("overwrite").saveAsTable("test.check_cvr_smooth_data20190329")
  }


  def calculateSmoothV2(rawData: DataFrame, minCV: Int, spark: SparkSession) = {
    val pcocData = calculatePCOCv2(rawData, minCV, spark)
    val jfbData = calculateJFB(rawData, spark)

    val result = pcocData
      .join(jfbData, Seq("unitid"), "inner")
      .selectExpr("cast(unitid as string) identifier", "pcoc", "jfb", "post_cvr")
      .filter(s"pcoc is not null and pcoc != 0")

    result
  }

  def calculateJFB(rawData: DataFrame, spark: SparkSession) = {
    val jfbData = rawData
      .groupBy("unitid")
      .agg(
        sum(col("price")).alias("total_price"),
        sum(col("bid")).alias("total_bid")
      )
      .select("unitid", "total_price", "total_bid")
      .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
      .select("unitid", "jfb")

    jfbData.show()

    jfbData
  }


  def calculatePCOCv2(rawData: DataFrame, minCV: Int, spark: SparkSession) = {
    val pcocData = rawData
      .groupBy("unitid")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        avg(col("exp_cvr")).alias("pre_cvr")
      )
      .select("unitid", "click", "cv", "pre_cvr")
      .filter(s"cv >= $minCV")
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .select("unitid", "post_cvr", "pre_cvr")
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("unitid", "pcoc", "post_cvr")

    pcocData.show(10)

    pcocData
  }


}