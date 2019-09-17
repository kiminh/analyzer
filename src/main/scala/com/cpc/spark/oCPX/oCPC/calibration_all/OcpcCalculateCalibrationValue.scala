package com.cpc.spark.oCPX.oCPC.calibration_all

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcCalculateCalibrationValue {
  def main(args: Array[String]): Unit = {
    /*
    计算计费比
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    // 兜底校准时长
    val hourInt = args(2).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt=$hourInt")

    val dataRaw = OcpcCalibrationBaseMain(date, hour, hourInt, spark).cache()
    val dataRaw1 = dataRaw
      .withColumn("identifier", concat_ws("-", col("userid"), col("conversion_goal"), col("media")))
      .select("identifier", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")
    val dataRaw2 = dataRaw
      .withColumn("identifier", concat_ws("-", col("conversion_goal"), col("media")))
      .select("identifier", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")

    val result = OcpcCalculateCalibrationValueMain(dataRaw1, 40, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_jfb_factor20190917b")
  }

  def OcpcCalculateCalibrationValueMain(dataRaw: DataFrame, minCV: Int, spark: SparkSession) = {
    val data = dataRaw
      .groupBy("identifier")
      .agg(
        sum(col("click")).alias("click"),
        sum(col("cv")).alias("cv"),
        sum(col("total_pre_cvr")).alias("total_pre_cvr"),
        sum(col("total_bid")).alias("total_bid"),
        sum(col("total_price")).alias("total_price")
      )
      .na.fill(0, Seq("cv"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
      .select("identifier", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "post_cvr", "pre_cvr", "pcoc", "jfb")
      .withColumn("min_cv", lit(minCV))
      .filter(s"cv > 0")
    data.show(10)

    data
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_jfb_factor20190917a")

    val resultDF = data
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
//      .filter(s"cv >= min_cv")
      .select("identifier", "jfb_factor", "cvr_factor", "post_cvr")


    resultDF
  }


  def OcpcCalibrationBaseMain(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
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
       """.stripMargin
    println(sqlRequest)
    val baseData = spark
      .sql(sqlRequest)
      .selectExpr("cast(unitid as string) identifier", "userid", "conversion_goal", "media", "isclick", "iscvr", "bid", "price", "exp_cvr", "date", "hour")



    // 计算结果
    val result = calculateParameter(baseData, spark)

    val resultDF = result
      .select("identifier", "userid", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")


    resultDF
  }


  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("identifier", "userid", "conversion_goal", "media", "date", "hour")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        sum(col("bid")).alias("total_bid"),
        sum(col("price")).alias("total_price"),
        sum(col("exp_cvr")).alias("total_pre_cvr")
      )
      .select("identifier", "userid", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")

    data
  }

}