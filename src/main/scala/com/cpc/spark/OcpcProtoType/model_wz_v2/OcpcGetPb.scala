package com.cpc.spark.OcpcProtoType.model_wz_v2

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.OcpcProtoType.model_wz_v2.OcpcCalculateCalibration.OcpcCalculateCalibrationMain



object OcpcGetPb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val highBidFactor = args(4).toDouble
    val lowBidFactor = args(5).toDouble
    val hourInt = args(6).toInt
    val conversionGoal = args(7).toInt
    val minCV = args(8).toInt
    val isHidden = 1

    // 主校准回溯时间长度
    val hourInt1 = args(9).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(10).toInt
    // 兜底校准时长
    val hourInt3 = args(11).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, media:$media, highBidFactor:$highBidFactor, lowBidFactor:$lowBidFactor, hourInt:$hourInt, conversionGoal:$conversionGoal, minCV:$minCV, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    val calibraionData = OcpcCalculateCalibrationMain(date, hour, conversionGoal, version, media, minCV, hourInt1, hourInt2, hourInt3, spark).cache()

    println(s"print result:")
    calibraionData.show(10)

    val resultDF = calibraionData
      .withColumn("high_bid_factor", lit(0.0))
      .withColumn("low_bid_factor", lit(0.0))
      .cache()

    resultDF.show(10)
    resultDF
      .select("identifier", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor")
      .withColumn("is_hidden", lit(isHidden))
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(5)
//      .write.mode("overwrite").saveAsTable("test.ocpc_param_calibration_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_param_calibration_hourly")


    println("successfully save data into hive")

  }



}


