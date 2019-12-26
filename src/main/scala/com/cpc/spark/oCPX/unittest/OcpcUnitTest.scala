package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.deepOcpc.calibration_v6.OcpcGetPb_retention.{OcpcCalibrationBase, calculateDataCvr, calculateDeepCvr}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcUnitTest {
  /*
  新增部分媒体id采用暗投
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
//    val version = "ocpctest"
//    val expTag = "weightv1"
//    val hourInt = 89

    println("parameters:")
    println(s"date=$date, hour=$hour")


    // base data
    val dataRaw = OcpcCalibrationBase(date, hour, 72, spark)

    val data = calculateDeepCvr(date, 3, spark)

    data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data201901226b")


  }

}


