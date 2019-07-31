package com.cpc.spark.oCPX.oCPC.calibration

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcBIDfactor._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcCVRfactorV2._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcCalibrationBaseDelay._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcJFBfactorV2._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcSmoothfactorV2._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPbV2 {
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

    // 主校准回溯时间长度
    val hourInt1 = args(7).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(8).toInt
    // 兜底校准时长
    val hourInt3 = args(9).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

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
      .cache()
    jfbData.show(10)

    val smoothDataRaw = OcpcSmoothfactorMain(date, hour, version, expTag, dataRaw1, dataRaw2, dataRaw3, spark)
    val smoothData = smoothDataRaw
      .withColumn("post_cvr", col("cvr"))
      .select("unitid", "conversion_goal", "exp_tag", "post_cvr", "smooth_factor")
      .cache()
    smoothData.show(10)

    val pcocDataRaw = OcpcCVRfactorMain(date, hour, version, expTag, dataRaw1, dataRaw2, dataRaw3, spark)
    val pcocData = pcocDataRaw
      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "cvr_factor")
      .cache()
    pcocData.show(10)

    val bidFactorDataRaw = OcpcBIDfactorMain(date, hour, version, expTag, bidFactorHourInt, spark)
    val bidFactorData = bidFactorDataRaw
      .select("unitid", "conversion_goal", "exp_tag", "high_bid_factor", "low_bid_factor")
      .cache()
    bidFactorData.show(10)

    val data = assemblyData(jfbData, smoothData, pcocData, bidFactorData, spark).cache()
    data.show(10)

    dataRaw1.unpersist()
    dataRaw2.unpersist()
    dataRaw3.unpersist()

    // 明投单元
    val resultUnhidden = data
      .withColumn("cpagiven", lit(1.0))
      .withColumn("is_hidden", lit(0))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    // 暗投单元
    val hiddenUnits = getCPAgiven(spark)
    val resultHidden = data
      .join(hiddenUnits, Seq("unitid"), "inner")
      .withColumn("is_hidden", lit(1))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")


    val resultDF = resultUnhidden
      .union(resultHidden)
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly_exp")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly_exp")


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

    data


  }

  def getCPAgiven(spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  avg(cpa) as cpagiven
         |FROM
         |  test.ocpc_auto_budget_hourly
         |WHERE
         |  industry in ('wzcp')
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest).cache()
    result.show(10)
    result
  }



}


