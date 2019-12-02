package com.cpc.spark.oCPX.unittest

import com.cpc.spark.oCPX.oCPC.calibration.OcpcCVRfactorV2.OcpcCVRfactorMain
import com.cpc.spark.oCPX.oCPC.calibration.OcpcCalibrationBase.OcpcCalibrationBaseMain
import com.cpc.spark.oCPX.oCPC.calibration.OcpcJFBfactorV2.OcpcJFBfactorMain
import com.cpc.spark.oCPX.oCPC.calibration_by_tag.OcpcGetPb_adtype15.{OcpcCVRfactor, OcpcCalibrationBase, OcpcJFBfactor, getDataByTimeSpan}
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
    val expTag = "adtype15"

    // 计算jfb_factor,cvr_factor,post_cvr
    val dataRaw1 = OcpcCalibrationBaseMain(date, hour, 24, spark).cache()
    dataRaw1.show(10)
    val dataRaw2 = OcpcCalibrationBaseMain(date, hour, 48, spark).cache()
    dataRaw2.show(10)
    val dataRaw3 = OcpcCalibrationBaseMain(date, hour, 72, spark).cache()
    dataRaw3.show(10)

    val pcocDataRaw1 = OcpcCVRfactorMain(date, hour, version, expTag, dataRaw1, dataRaw2, dataRaw3, spark)
    val pcocData1 = pcocDataRaw1
      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "cvr_factor")
      .cache()
    pcocData1.show(10)

    pcocData1
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191202a")

    /*****************************************/
    // 基础数据
    val dataRaw = OcpcCalibrationBase(date, hour, 72, spark).cache()
    dataRaw.show(10)

    // 计费比系数模块
    val pcocDataRaw2 = OcpcCVRfactor(date, hour, expTag, dataRaw, 24, 48, 72, spark)
    val pcocData2 = pcocDataRaw2
      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "cvr_factor")
      .cache()
    pcocData2.show(10)

    pcocData2
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191202b")

  }

}


