package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.deepOcpc.calibration_v7.OcpcGetPb_retention.{OcpcCalibrationBase, calculateCvrFactor, calculateCvrPart1, calculateCvrPart2, calculateDeepCvr}
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

    println("parameters:")
    println(s"date=$date, hour=$hour")

    val dataRaw = OcpcCalibrationBase(date, hour, 96, spark)
//    // calculate deep_cvr
//    val deepCvr = calculateDeepCvr(date, 3, spark)
//
//    // calculate cv2_t1
//    val data1 = calculateCvrPart1(dataRaw, deepCvr, 10, spark)
//
//    // calculate cv2_t2 ~ cv2_t4
//    val data2 = calculateCvrPart2(dataRaw, 20, spark)
//
//    // data join
//    val data = data1.union(data2)

    val cvrData = calculateCvrFactor(dataRaw, date, hour, spark)


    cvrData
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20200102b")




  }

}


