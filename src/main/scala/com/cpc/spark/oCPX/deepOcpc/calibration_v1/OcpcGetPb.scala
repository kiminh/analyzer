package com.cpc.spark.oCPX.deepOcpc.calibration_v1

import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcCVRfactor._
import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcCalibrationBase._
import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcJFBfactor._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.oCPX.OcpcTools._


object OcpcGetPb {
  /*
  采用基于后验激活率的复合校准策略
  jfb_factor：正常计算
  cvr_factor：
  cvr_factor = (deep_cvr * post_cvr1) / pre_cvr1
  smooth_factor = 0.3
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString

    // 主校准回溯时间长度
    val hourInt = args(5).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt:$hourInt")

    // 计算计费比系数、后验激活转化率、先验点击次留率
    val data1 = getData1(date, hour, hourInt, spark)

    // 计算自然天激活次留率

    // 计算cvr校准系数

    // 数据组装

    // 输出到结果表 dl_cpc.ocpc_deep_pb_data_hourly
  }

  def getData1(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    // 计算计费比系数、后验激活转化率、先验点击次留率
    val rawData = getBaseData(hourInt, date, hour, spark)

    val data  =rawData
      .filter(s"isclick=1 and is_deep_ocpc = 1")
      .groupBy("identifier", "conversion_goal", "media", "date", "hour")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        sum(col("bid")).alias("total_bid"),
        sum(col("price")).alias("total_price")
      )
      .select("identifier", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "date", "hour")


  }


}
