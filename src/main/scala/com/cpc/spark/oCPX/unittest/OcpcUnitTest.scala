package com.cpc.spark.oCPX.unittest



import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getBaseDataRealtime
import com.cpc.spark.oCPX.deepOcpc.calibration_v8.OcpcGetPb_retention.{OcpcCalibrationBase, calculateCvrPart1, calculateCvrPart2, calculateDeepCvr}
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

    val dataRaw = OcpcCalibrationBase(date, hour, 96, spark)

    // calculate cv2_t2 ~ cv2_t4
    val data2 = calculateCvrPart2(dataRaw, 20, spark)


    data2
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20200102c")



  }

}


