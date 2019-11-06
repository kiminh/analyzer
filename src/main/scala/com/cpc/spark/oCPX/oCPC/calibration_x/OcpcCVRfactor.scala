package com.cpc.spark.oCPX.oCPC.calibration_x

import com.cpc.spark.oCPX.OcpcTools.{getBaseData, udfAdslotTypeMapAs, udfMediaName, udfSetExpTag}
import com.cpc.spark.oCPX.oCPC.calibration_alltype.OcpcCalibrationBase._
import com.cpc.spark.oCPX.oCPC.calibration_alltype.udfs.udfGenerateId
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcCVRfactor {
  def main(args: Array[String]): Unit = {
    /*
    校准策略：
    主校准 + 备用校准：
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // 计算日期周期
    // bash: 2019-01-02 12 1 qtt_demo qtt
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    // 主校准回溯时间长度
    val hourInt = args(4).toInt


    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, expTag=$expTag, hourInt=$hourInt")

    val dataRaw = OcpcCalibrationBaseMain(date, hour, hourInt, spark).cache()

//    val result = OcpcCVRfactorMain(date, hour, version, expTag, dataRaw, spark)
    dataRaw
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_cvr_factor20190723b")

  }

//  def OcpcCVRfactorMain(date: String, hour: String, version: String, expTag: String, dataRaw: DataFrame, spark: SparkSession) = {
//    // cvr实验配置文件
//    val data = dataRaw
//        .withColumn("media", udfMediaName()(col("media")))
//        .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
//        .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
//        .na.fill(40, Seq("min_cv"))
//        .filter(s"cv > 0")
//    data1.show(10)
//  }

  def OcpcCalibrationBaseMain(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseDataRaw = getBaseData(hourInt, date, hour, spark)
    val baseData = baseDataRaw

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


