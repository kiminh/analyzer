package com.cpc.spark.oCPX.oCPC.calibration

import com.cpc.spark.oCPX.oCPC.calibration.OcpcJFBfactor._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcSmoothfactor._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcCVRfactor._
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
    val expTag = args(3).toString
    val jfbHourInt = args(4).toInt
    val smoothHourInt = args(5).toInt
    val isHidden = 0

    // 主校准回溯时间长度
    val hourInt1 = args(6).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(7).toInt
    // 兜底校准时长
    val hourInt3 = args(8).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    val jfbData = OcpcJFBfactorMain(date, hour, version, expTag, jfbHourInt, spark)
    val smoothData = OcpcSmoothFactorMain(date, hour, version, expTag, smoothHourInt, spark)
    val pcocData = OcpcCVRfactorMain(date, hour, version, expTag, hourInt1, hourInt2, hourInt3, spark)

//    println(s"print result:")
//    calibraionData.show(10)
//    factorData.show(10)

//    val resultDF = calibraionData
//      .join(factorData.select("identifier", "conversion_goal", "high_bid_factor", "low_bid_factor"), Seq("identifier", "conversion_goal"), "left_outer")
//      .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor"))
//      .withColumn("cpagiven", lit(1.0))
//      .cache()
//
//    resultDF.show(10)
//    resultDF
//      .select("identifier", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor", "cpagiven", "conversion_goal")
//      .withColumn("is_hidden", lit(isHidden))
//      .withColumn("exp_tag", lit(expTag))
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .withColumn("version", lit(version))
//      .select("identifier", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor", "cpagiven", "is_hidden", "exp_tag", "conversion_goal", "date", "hour", "version")
//      .repartition(5)
////      .write.mode("overwrite").insertInto("test.ocpc_param_calibration_hourly_v2")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_param_calibration_hourly_v2")
//
//
//    println("successfully save data into hive")

  }



}


