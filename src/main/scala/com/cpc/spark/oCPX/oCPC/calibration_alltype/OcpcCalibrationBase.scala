package com.cpc.spark.oCPX.oCPC.calibration_alltype

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.oCPC.calibration_alltype.udfs._
import com.typesafe.config.ConfigFactory
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
    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt:$hourInt")

    val result1 = OcpcCalibrationBaseMain(date, hour, hourInt, spark)
    val result2 = OcpcCalibrationBaseMainOnlySmooth(date, hour, hourInt, spark)

    result1
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_base_factor20190902a")
    result2
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_base_factor20190902b")

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
      .withColumn("adslot_type", udfAdslotTypeMapAs()(col("adslot_type")))
      .withColumn("identifier", udfGenerateId()(col("unitid"), col("adslot_type")))

    // 计算结果
    val result = calculateParameter(baseData, spark)

    val resultDF = result
      .select("identifier", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc", "acb", "acp")


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
      .withColumn("bid", udfCalculateBidWithHiddenTax()(col("date"), col("bid"), col("hidden_tax")))
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))
      .withColumn("adslot_type", udfAdslotTypeMapAs()(col("adslot_type")))
      .withColumn("identifier", udfGenerateId()(col("unitid"), col("adslot_type")))

    // 计算结果
    val result = calculateParameter(baseData, spark)

    val resultDF = result
      .select("identifier", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc", "acb", "acp")


    resultDF
  }


  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("identifier", "conversion_goal", "media")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        avg(col("bid")).alias("acb"),
        avg(col("price")).alias("acp"),
        avg(col("exp_cvr")).alias("pre_cvr")
      )
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("identifier", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc", "acb", "acp")

    data
  }

}