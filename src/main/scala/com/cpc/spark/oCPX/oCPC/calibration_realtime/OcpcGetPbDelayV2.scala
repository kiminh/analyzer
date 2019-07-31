package com.cpc.spark.oCPX.oCPC.calibration_realtime

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.oCPC.calibration.OcpcBIDfactor._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcCalibrationBaseDelay.OcpcCalibrationBaseDelayMain
import com.cpc.spark.oCPX.oCPC.calibration.OcpcJFBfactorV2._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcSmoothfactorV2._
import com.cpc.spark.oCPX.oCPC.calibration_realtime.OcpcCVRfactorDelay._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPbDelayV2 {
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

    val data = OcpcGetPbDelayMain(date, hour, version, expTag, jfbHourInt, smoothHourInt, bidFactorHourInt, hourInt1, hourInt2, hourInt3, 6, spark)

    val resultDF = data
      .withColumn("is_hidden", lit(isHidden))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    resultDF
      .repartition(5)
//      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly")




  }

  def OcpcGetPbDelayMain(originalDate: String, originalHour: String, version: String, expTag: String, jfbHourInt: Int, smoothHourInt: Int, bidFactorHourInt: Int, hourInt1: Int, hourInt2: Int, hourInt3: Int, delayHour: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = originalDate + " " + originalHour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -delayHour)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date = tmpDateValue(0)
    val hour = tmpDateValue(1)


    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3, originalDate=$originalDate, originalHour=$originalHour, delayHour=$delayHour")

    // 计算jfb_factor,cvr_factor,post_cvr
    val dataRaw1 = OcpcCalibrationBaseDelayMain(date, hour, hourInt1, spark).cache()
    dataRaw1.show(10)
    val dataRaw2 = OcpcCalibrationBaseDelayMain(date, hour, hourInt2, spark).cache()
    dataRaw2.show(10)
    val dataRaw3 = OcpcCalibrationBaseDelayMain(date, hour, hourInt3, spark).cache()
    dataRaw3.show(10)

    val jfbDataRaw = OcpcJFBfactorMain(date, hour, version, expTag, dataRaw1, dataRaw2, dataRaw3, spark)
    val jfbData = jfbDataRaw
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .select("unitid", "conversion_goal", "exp_tag", "jfb_factor")

    val smoothDataRaw = OcpcSmoothfactorMain(date, hour, version, expTag, dataRaw1, dataRaw2, dataRaw3, spark)
    val smoothData = smoothDataRaw
      .withColumn("post_cvr", col("cvr"))
      .select("unitid", "conversion_goal", "exp_tag", "post_cvr", "smooth_factor")

    val pcocDataRaw = OcpcCVRfactorDelayMain(date, hour, version, expTag, hourInt1, hourInt2, hourInt3, spark)
    val pcocData = pcocDataRaw
      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "cvr_factor")

    val bidFactorDataRaw = OcpcBIDfactorMain(date, hour, version, expTag, bidFactorHourInt, spark)
    val bidFactorData = bidFactorDataRaw
      .select("unitid", "conversion_goal", "exp_tag", "high_bid_factor", "low_bid_factor")

    val data = assemblyData(jfbData, smoothData, pcocData, bidFactorData, spark)

    val resultDF = data
      .withColumn("cpagiven", lit(1.0))
      .withColumn("version", lit(version))
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "exp_tag", "version")

    resultDF

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


