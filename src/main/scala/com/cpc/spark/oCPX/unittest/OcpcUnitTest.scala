package com.cpc.spark.oCPX.unittest

import com.cpc.spark.oCPX.deepOcpc.calibration_v4.OcpcGetPb_retention.getPreCvrData
import com.cpc.spark.oCPX.oCPC.calibration.OcpcCVRfactorV2.OcpcCVRfactorMain
import com.cpc.spark.oCPX.oCPC.calibration.OcpcCalibrationBase.OcpcCalibrationBaseMain
import com.cpc.spark.oCPX.oCPC.calibration.OcpcJFBfactorV2.OcpcJFBfactorMain
import com.cpc.spark.oCPX.oCPC.calibration_by_tag.OcpcGetPb_adtype15.{OcpcCVRfactor, OcpcCalibrationBase, OcpcJFBfactor, getDataByTimeSpan}
import com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction.prepareLabel.prepareLabelMain
import com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction.v3.OcpcMaeMonitor.{getBaselinePcoc, getPredPcoc}
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

    println("parameters:")
    println(s"date=$date, hour=$hour")

    val dataRaw = getPredPcoc(date, hour, 12, 4, "ocpctest", "v3", spark)

    dataRaw
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191204b")

  }

}


