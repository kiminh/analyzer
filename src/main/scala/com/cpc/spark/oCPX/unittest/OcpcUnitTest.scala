package com.cpc.spark.oCPX.unittest

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcBIDfactor._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcCVRfactorRealtime._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcCalculateCalibrationValue._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcJFBfactor._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcSmoothfactor._
import com.cpc.spark.oCPX.oCPC.calibration_by_tag.OcpcGetPb_baseline_others.getBaseDataDelayOther
import com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction.v3.prepareTrainingSample.getFeatureData
import com.cpc.spark.oCPX.oCPC.calibration_x.realtime.pcoc_calibration.OcpcGetPb_realtime.{OcpcBIDfactor, OcpcCVRfactor, OcpcJFBfactor}
import com.cpc.spark.oCPX.oCPC.calibration_x.realtime.pid_calibration.OcpcGetPb_pidrealtime.{OcpcPIDfactor, calculateError}
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
    val hourInt = args(2).toInt
    val minCV = 10

    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt=$hourInt")

    val version = "ocpctest"
    val expTag = "v3"

    val baseDataRaw = getFeatureData(date, hour, hourInt, version, expTag, spark)

    baseDataRaw
//      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191127c")

  }

}


