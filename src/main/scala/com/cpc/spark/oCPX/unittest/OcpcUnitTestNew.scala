package com.cpc.spark.oCPX.unittest

import com.cpc.spark.oCPX.deepOcpc.calibration_v9.OcpcGetPb_retention.{OcpcCalibrationBase, calculateCvrPart1, calculateCvrPart2, calculateDeepCvr, getDataByHourDiff, getRecallValue}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, sum, when}


object OcpcUnitTestNew {
  /*
  新增部分媒体id采用暗投
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = 24

    println("parameters:")
    println(s"date=$date, hour=$hour")


    val dataRaw = OcpcCalibrationBase(date, hour, 96, spark)

    // calculate deep_cvr
    val deepCvr = calculateDeepCvr(date, 3, spark)

    val data2 = calculateCvrPart2(dataRaw, deepCvr, 20, spark)

    data2
      .write.mode("overwrite").saveAsTable("test.check_ocpc_cali_exp_data20200203d")



  }

}


