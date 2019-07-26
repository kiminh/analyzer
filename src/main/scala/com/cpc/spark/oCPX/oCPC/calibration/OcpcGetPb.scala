package com.cpc.spark.oCPX.oCPC.calibration

import com.cpc.spark.oCPX.oCPC.calibration.OcpcJFBfactor._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcSmoothfactor._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcCVRfactor._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcBIDfactor._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
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
    val bidFactorHourInt = args(6).toInt
    val isHidden = 0

    // 主校准回溯时间长度
    val hourInt1 = args(7).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(8).toInt
    // 兜底校准时长
    val hourInt3 = args(9).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    val jfbDataRaw = OcpcJFBfactorMain(date, hour, version, expTag, jfbHourInt, spark)
    val jfbData = jfbDataRaw
      .withColumn("jfb_factor", col("total_bid") * 1.0 / col("total_price"))
      .select("unitid", "conversion_goal", "exp_tag", "jfb_factor")

    val smoothDataRaw = OcpcSmoothFactorMain(date, hour, version, expTag, smoothHourInt, spark)
    val smoothData = smoothDataRaw
      .withColumn("post_cvr", col("cvr"))
      .select("unitid", "conversion_goal", "exp_tag", "post_cvr", "smooth_factor")

    val pcocDataRaw = OcpcCVRfactorMain(date, hour, version, expTag, hourInt1, hourInt2, hourInt3, spark)
    val pcocData = pcocDataRaw
      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "cvr_factor")

    val bidFactorDataRaw = OcpcBIDfactorMain(date, hour, version, expTag, bidFactorHourInt, spark)
    val bidFactorData = bidFactorDataRaw
      .select("unitid", "conversion_goal", "exp_tag", "high_bid_factor", "low_bid_factor")

    val data = assemblyData(jfbData, smoothData, pcocData, bidFactorData, spark)

    val resultDF = data
      .withColumn("cpagiven", lit(1.0))
      .withColumn("is_hidden", lit(isHidden))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    resultDF
      .repartition(5)
//      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly")


  }

  def assemblyData(jfbData: DataFrame, smoothData: DataFrame, pcocData: DataFrame, bidFactorData: DataFrame, spark: SparkSession) = {
    // 组装数据
    val data = jfbData
      .join(pcocData, Seq("unitid", "conversion_goal", "exp_tag"), "outer")
      .join(smoothData, Seq("unitid", "conversion_goal", "exp_tag"), "outer")
      .join(bidFactorData, Seq("unitid", "conversion_goal", "exp_tag"), "outer")
      .select("unitid", "conversion_goal", "exp_tag", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
      .na.fill(1.0, Seq("jfb_factor", "cvr_factor", "high_bid_factor", "low_bid_factor"))
      .na.fill(0.0, Seq("post_cvr", "smooth_factor"))
      .cache()

    data.show(10)
    data




  }



}


