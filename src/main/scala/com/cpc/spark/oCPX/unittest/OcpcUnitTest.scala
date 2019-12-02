package com.cpc.spark.oCPX.unittest

import com.cpc.spark.oCPX.oCPC.calibration.OcpcCalibrationBase.OcpcCalibrationBaseMain
import com.cpc.spark.oCPX.oCPC.calibration_by_tag.OcpcGetPb_adtype15.{OcpcCalibrationBase, getDataByTimeSpan}
import com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction.prepareLabel.prepareLabelMain
import com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction.v3.prepareTrainingSample.{getFeatureData, udfAddHour, udfStringListAppend}
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
    val hourInt1 = args(2).toInt
    val minCV = 10

    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt=$hourInt1")

    val version = "ocpctest"
    val expTag = "v3"

    val dataRaw1 = OcpcCalibrationBaseMain(date, hour, hourInt1, spark).cache()

    val dataRaw2 = OcpcCalibrationBase(date, hour, hourInt1, spark).cache()
    val data2  = getDataByTimeSpan(dataRaw2, date, hour, hourInt1, spark)

    dataRaw1
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191202a")

    data2
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191202b")

  }

}


