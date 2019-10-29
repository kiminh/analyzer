package com.cpc.spark.oCPX.deepOcpc.calibration_v1

import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcCVRfactor._
import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcCalibrationBase._
import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcJFBfactor._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.deepOcpc.calibration_v1.OcpcShallowFactor._
import com.cpc.spark.oCPX.deepOcpc.calibration_v1.OcpcRetentionFactor._


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
    val hourInt = args(4).toInt
    val minCV = args(5).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt:$hourInt")

    // 计算计费比系数、后验激活转化率、先验点击次留率
    val data1 = OcpcShallowFactorMain(date, hour, hourInt, expTag, minCV, spark)

    // 计算自然天激活次留率
    val data2 = OcpcRetentionFactorMain(date, expTag, minCV, spark)

    // 计算cvr校准系数

    // 数据组装

    // 输出到结果表 dl_cpc.ocpc_deep_pb_data_hourly
  }

}
