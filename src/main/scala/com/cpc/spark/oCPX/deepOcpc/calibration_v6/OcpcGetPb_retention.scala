package com.cpc.spark.oCPX.deepOcpc.calibration_v6

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcRetentionFactor._
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcShallowFactor._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_retention {
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
    val minCV1 = args(5).toInt
    val minCV2 = args(6).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt:$hourInt")

    /*
    jfb_factor calculation
     */

    /*
    cvr factor calculation
     */



  }

  def calculateCvrFactor(date: String, hour: String, spark: SparkSession) = {
    /*
    method to calculate cvr_factor: predict the retention cv

    cvr_factor = post_cvr1 * deep_cvr / pre_cvr2

    deep_cvr: calculate by natural day
    post_cvr1, pre_cvr2: calculate by sliding time window
     */

    // calculate deep_cvr
    val deepCvr = calculateDeepCvr(date, 3, spark)


  }

  def calculateDeepCvr(date: String, dayInt: Int, spark: SparkSession) = {
    
  }


}
