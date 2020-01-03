package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.deepOcpc.calibration_baseline.OcpcGetPb_pay.{OcpcCalibrationFactor, assemblyData, calculateCalibrationValue}
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


    val rawData = OcpcCalibrationFactor(date, hour, 72, 20, spark)

    // 计算cvr校准系数
    val data = calculateCalibrationValue(rawData, spark)


    // 数据组装
    val resultData = assemblyData(data, spark)

    resultData
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20200103b")



  }

}


