package com.cpc.spark.oCPX.oCPC.calibration_all

import com.cpc.spark.oCPX.OcpcTools._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcCalibrationBase {
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
    val isDelay = args(3).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt:$hourInt")

    val result1 = OcpcCalibrationBaseDelayMain(date, hour, hourInt, spark)
    val result2 = OcpcCalibrationBaseDelayMainOnlySmooth(date, hour, hourInt, spark)

    result1
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_base_factor20190903a")
    result2
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_base_factor20190903b")


//    var result = OcpcCalibrationBaseMain(date, hour, hourInt, spark)
//    if (isDelay == 1) {
//      result = OcpcCalibrationBaseDelayMain(date, hour, hourInt, spark)
//    }
//    result
//      .repartition(10).write.mode("overwrite").saveAsTable("test.check_base_factor20190731a")
  }

  def OcpcCalibrationBaseDelayMainOnlySmooth(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseDataRaw = getBaseDataDelay(hourInt, date, hour, spark)
    baseDataRaw.createOrReplaceTempView("base_data_raw")

    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  base_data_raw
         |WHERE
         |  (media = 'qtt' and ocpc_expand = 0 AND array_contains(split(expids, ','), '35456'))
         |OR
         |  media in ('hottopic', 'novel')
       """.stripMargin
    println(sqlRequest)
    val baseData = spark
      .sql(sqlRequest)
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "media", "isclick", "iscvr", "bid", "price", "exp_cvr", "date", "hour")

    // 计算结果
    val result = calculateParameter(baseData, spark)

    val resultDF = result
      .select("identifier", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")


    resultDF
  }


  def OcpcCalibrationBaseDelayMain(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseDataRaw = getBaseDataDelay(hourInt, date, hour, spark)
    val baseData = baseDataRaw
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "media", "isclick", "iscvr", "bid", "price", "exp_cvr", "date", "hour")

    // 计算结果
    val result = calculateParameter(baseData, spark)

    val resultDF = result
      .select("identifier", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")


    resultDF
  }

  def OcpcCalibrationBaseMainOnlySmooth(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseDataRaw = getBaseData(hourInt, date, hour, spark)
    baseDataRaw.createOrReplaceTempView("base_data_raw")

    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  base_data_raw
         |WHERE
         |  (media = 'qtt' and ocpc_expand = 0 AND array_contains(split(expids, ','), '35456'))
         |OR
         |  media in ('hottopic', 'novel')
       """.stripMargin
    println(sqlRequest)
    val baseData = spark
      .sql(sqlRequest)
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "media", "isclick", "iscvr", "bid", "price", "exp_cvr", "date", "hour")

    // 计算结果
    val result = calculateParameter(baseData, spark)

    val resultDF = result
      .select("identifier", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")


    resultDF
  }


  def OcpcCalibrationBaseMain(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseDataRaw = getBaseData(hourInt, date, hour, spark)
    val baseData = baseDataRaw
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "media", "isclick", "iscvr", "bid", "price", "exp_cvr", "date", "hour")

    // 计算结果
    val result = calculateParameter(baseData, spark)

    val resultDF = result
      .select("identifier", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")


    resultDF
  }


  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("identifier", "conversion_goal", "media", "date", "hour")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        sum(col("bid")).alias("total_bid"),
        sum(col("price")).alias("total_price"),
        sum(col("exp_cvr")).alias("total_pre_cvr")
      )
      .select("identifier", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")

    data
  }

}