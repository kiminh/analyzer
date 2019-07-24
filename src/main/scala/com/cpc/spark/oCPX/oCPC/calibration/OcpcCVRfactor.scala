package com.cpc.spark.oCPX.oCPC.calibration

import com.cpc.spark.oCPX.oCPC.calibration.OcpcCvrFactorBase._
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
    val hourInt1 = args(4).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(5).toInt
    // 兜底校准时长
    val hourInt3 = args(6).toInt


    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, expTag=$expTag, hourInt1=$hourInt1, hourInt2=$hourInt2, hourInt3=$hourInt3")
    // 抽取媒体id

    val result = OcpcCVRfactorMain(date, hour, version, expTag, hourInt1, hourInt2, hourInt3, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_cvr_factor20190723b")

  }

  def OcpcCVRfactorMain(date: String, hour: String, version: String, expTag: String, hourInt1: Int, hourInt2: Int, hourInt3: Int, spark: SparkSession) = {
//    // 抽取媒体id
//    val conf = ConfigFactory.load("ocpc")
//    val conf_key = "medias.total.media_selection"
//    val mediaSelection = conf.getString(conf_key)

    val data1 = OcpcCvrFactorBaseMain(date, hour, version, expTag, hourInt1, spark).cache()
    data1.show(10)
    val data2 = OcpcCvrFactorBaseMain(date, hour, version, expTag, hourInt2, spark).cache()
    data2.show(10)
    val data3 = OcpcCvrFactorBaseMain(date, hour, version, expTag, hourInt3, spark).cache()
    data3.show(10)

    val calibration1 = calculateCalibrationValue(data1, data2, spark)
    val calibrationNew = data3
      .withColumn("pcoc_new", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "version", "pcoc_new")

    val calibration = calibrationNew
      .join(calibration1, Seq("unitid", "conversion_goal", "exp_tag", "version"), "left_outer")
      .select("unitid", "conversion_goal", "exp_tag", "version", "pcoc_new", "pcoc3")
      .withColumn("pcoc", when(col("pcoc3").isNotNull, col("pcoc3")).otherwise(col("pcoc_new")))
      .cache()

    calibration.show(10)
    data1.unpersist()
    data2.unpersist()
    data3.unpersist()

    val resultDF = calibration
      .select("unitid", "conversion_goal", "exp_tag", "version", "pcoc")

    resultDF

  }

  def calculateCalibrationValue(dataRaw1: DataFrame, dataRaw2: DataFrame, spark: SparkSession) = {
    /*
    "identifier", "click", "cv", "pre_cvr", "total_price", "total_bid"
     */

    // 主校准模型
    val data1 = dataRaw1
      .withColumn("pcoc1", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "version", "pcoc1")

    // 备用校准模型
    val data2 = dataRaw2
      .withColumn("pcoc2", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "version", "pcoc2")

    // 数据表关联
    val data = data2
      .join(data1, Seq("unitid", "conversion_goal", "exp_tag", "version"), "left_outer")
      .na.fill(0, Seq("flag"))
      .withColumn("pcoc3", when(col("pcoc1").isNotNull, col("pcoc1")).otherwise(col("pcoc2")))

    data.show()

    data

  }



}


