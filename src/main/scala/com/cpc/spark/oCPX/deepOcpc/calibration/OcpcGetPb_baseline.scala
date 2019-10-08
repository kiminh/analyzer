package com.cpc.spark.oCPX.deepOcpc.calibration

import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcCVRfactor._
import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcCalibrationBase._
import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcJFBfactor._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_baseline {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    val bidFactorHourInt = args(4).toInt

    // 主校准回溯时间长度
    val hourInt1 = args(5).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(6).toInt
    // 兜底校准时长
    val hourInt3 = args(7).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    // 计算jfb_factor,cvr_factor,post_cvr
    val dataRaw = OcpcCalibrationBaseDelayMain(date, hour, hourInt3, spark).cache()
    dataRaw.show(10)

    dataRaw
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_exp_data20190912a")


    val jfbDataRaw = OcpcJFBfactorMain(date, hour, version, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    val jfbData = jfbDataRaw
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor")
      .cache()
    jfbData.show(10)
    jfbData
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_exp_data20190912b")

    val pcocDataRaw = OcpcCVRfactorMain(date, hour, version, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    val pcocData = pcocDataRaw
      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
      .select("identifier", "conversion_goal", "exp_tag", "cvr_factor")
      .cache()
    pcocData.show(10)
    pcocData
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_exp_data20190912f")

    val data = assemblyData(jfbData, pcocData, spark).cache()
    data.show(10)

    dataRaw.unpersist()

    // 明投单元
    val result = data
      .withColumn("cpagiven", lit(1.0))
      .withColumn("is_hidden", lit(0))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    val resultDF = result
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    resultDF
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly_exp_alltype")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly_exp_alltype")


  }


  def assemblyData(jfbData: DataFrame, pcocData: DataFrame, spark: SparkSession) = {
    // 组装数据
    val data = pcocData
      .filter(s"cvr_factor is not null")
      .join(jfbData, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .withColumn("post_cvr", lit(0.0))
      .withColumn("smooth_factor", lit(1.0))
      .withColumn("high_bid_factor", lit(1.0))
      .withColumn("low_bid_factor", lit(1.0))
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
      .na.fill(1.0, Seq("jfb_factor", "cvr_factor", "high_bid_factor", "low_bid_factor"))
      .na.fill(0.0, Seq("post_cvr", "smooth_factor"))

    data
  }



}


