package com.cpc.spark.OcpcProtoType.model_v5

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
//import com.cpc.spark.OcpcProtoType.model_v5.OcpcSmoothFactor


object OcpcCalculateCalibration {
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
    val conversionGoal = args(2).toInt
    val version = args(3).toString
    val media = args(4).toString
    val minCV = args(5).toString

    // 主校准回溯时间长度
    val hourInt1 = args(6).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(7).toInt


    println("parameters:")
    println(s"date=$date, hour=$hour, conversionGoal=$conversionGoal, version=$version, media=$media, hourInt1=$hourInt1, hourInt2=$hourInt2")
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)
    val cvrType = "cvr" + conversionGoal.toString

    val data1 = OcpcSmoothFactor.OcpcSmoothFactor(date, hour, version, media, hourInt1, cvrType, spark).cache()
    data1.show(10)
    val data2 = OcpcSmoothFactor.OcpcSmoothFactor(date, hour, version, media, hourInt2, cvrType, spark).cache()
    data2.show(10)

    val calibration1 = calculateCalibrationValue(data1, date, hour, spark)



  }

  def calculateCalibrationValue(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    "identifier", "click", "cv", "pre_cvr", "total_price", "total_bid"
     */
    val data = rawData
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
  }



}


