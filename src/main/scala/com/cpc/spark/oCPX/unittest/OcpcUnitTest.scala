package com.cpc.spark.oCPX.unittest



import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getBaseDataRealtime
import com.cpc.spark.oCPX.oCPC.calibration_x.realtime.pcoc_calibration.OcpcGetPb_weightv2.{OcpcCVRfactor, OcpcRealtimeCalibrationBase}
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

    val realtimeDataRaw = OcpcRealtimeCalibrationBase(date, hour, 84, spark).cache()
    realtimeDataRaw.show(10)
    val pcocDataRaw = OcpcCVRfactor(realtimeDataRaw, "weightv2", spark)

    pcocDataRaw
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20200102b")



  }

}


