package com.cpc.spark.OcpcProtoType.model_v5

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.model_v5.OcpcCalculateCalibration.OcpcCalculateCalibrationMain
import com.cpc.spark.OcpcProtoType.model_v5.OcpcRangeCalibration.OcpcRangeCalibrationMain
import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcGetPbPID {
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
    val expTag = args(9).toString
    val isHidden = 0

    // 主校准回溯时间长度
    val hourInt1 = args(10).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(11).toInt
    // 兜底校准时长
    val hourInt3 = args(12).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, media:$media, highBidFactor:$highBidFactor, lowBidFactor:$lowBidFactor, hourInt:$hourInt, conversionGoal:$conversionGoal, minCV:$minCV, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    val calibraionData = OcpcCalculateCalibrationMain(date, hour, conversionGoal, version, media, minCV, hourInt1, hourInt2, hourInt3, spark).cache()
    val factorData = OcpcRangeCalibrationMain(date, hour, version, media, highBidFactor, lowBidFactor, hourInt, conversionGoal, minCV, spark).cache()

    println(s"print result:")
    calibraionData.show(10)
    factorData.show(10)

    val resultDF = calibraionData
      .join(factorData.select("identifier", "high_bid_factor", "low_bid_factor"), Seq("identifier"), "left_outer")
      .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor"))
      .withColumn("cpagiven", lit(1.0))
      .cache()

    resultDF.show(10)
    resultDF
      .select("identifier", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor", "cpagiven")
      .withColumn("is_hidden", lit(isHidden))
      .withColumn("exp_tag", lit(expTag))
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(5)
//      .write.mode("overwrite").saveAsTable("test.ocpc_param_calibration_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_param_calibration_hourly_v2")


    println("successfully save data into hive")

  }



}


