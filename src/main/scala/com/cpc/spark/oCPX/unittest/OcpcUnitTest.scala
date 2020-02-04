package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.oCPC.calibration_by_tag.OcpcGetPb_weightv6.OcpcRealtimeCalibrationBase
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object OcpcUnitTest {
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



    val realtimeDataRaw = OcpcRealtimeCalibrationBase(date, hour, 100, spark).cache()

    realtimeDataRaw
      .write.mode("overwrite").saveAsTable("test.check_ocpc_cali_exp_data20200204a")



  }

}


