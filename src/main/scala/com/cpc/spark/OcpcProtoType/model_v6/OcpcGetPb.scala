package com.cpc.spark.OcpcProtoType.model_v6

import com.cpc.spark.OcpcProtoType.model_v6.OcpcCalculateCalibration.OcpcCalculateCalibrationMain
import com.cpc.spark.OcpcProtoType.model_v6.OcpcRangeCalibration.OcpcRangeCalibrationMain
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


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
    val minCV = args(7).toInt
    val expTag = args(8).toString
    val isHidden = 0

    // 主校准回溯时间长度
    val hourInt1 = args(9).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(10).toInt
    // 兜底校准时长
    val hourInt3 = args(11).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, media:$media, highBidFactor:$highBidFactor, lowBidFactor:$lowBidFactor, hourInt:$hourInt, minCV:$minCV, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    val calibraionData = OcpcCalculateCalibrationMain(date, hour, version, media, minCV, hourInt1, hourInt2, hourInt3, spark)
    val factorData = OcpcRangeCalibrationMain(date, hour, version, media, highBidFactor, lowBidFactor, hourInt, 40, spark)

//    println(s"print result:")
//    calibraionData.show(10)
//    factorData.show(10)

    val resultDF = calibraionData
      .join(factorData.select("identifier", "high_bid_factor", "low_bid_factor"), Seq("identifier", "conversion_goal"), "left_outer")
      .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor"))
      .withColumn("cpagiven", lit(1.0))
      .cache()

    resultDF.show(10)
    resultDF
      .select("identifier", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor", "cpagiven", "conversion_goal")
      .withColumn("is_hidden", lit(isHidden))
      .withColumn("exp_tag", lit(expTag))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("identifier", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor", "cpagiven", "is_hidden", "exp_tag", "conversion_goal", "date", "hour", "version")
      .repartition(5)
      .write.mode("overwrite").insertInto("test.ocpc_param_calibration_hourly_v2")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_param_calibration_hourly_v2")


    println("successfully save data into hive")

  }



}


