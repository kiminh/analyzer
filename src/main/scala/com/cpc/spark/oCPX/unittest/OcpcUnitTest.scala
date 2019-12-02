package com.cpc.spark.oCPX.unittest

import com.cpc.spark.oCPX.oCPC.calibration.OcpcCalibrationBase.OcpcCalibrationBaseMain
import com.cpc.spark.oCPX.oCPC.calibration.OcpcJFBfactorV2.OcpcJFBfactorMain
import com.cpc.spark.oCPX.oCPC.calibration_by_tag.OcpcGetPb_adtype15.{OcpcCalibrationBase, OcpcJFBfactor, getDataByTimeSpan}
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

    val jfbDataRaw1 = OcpcJFBfactorMain(date, hour, version, expTag, dataRaw1, dataRaw2, dataRaw3, spark)
    val jfbData1 = jfbDataRaw1
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .select("unitid", "conversion_goal", "exp_tag", "jfb_factor")

    jfbData1
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191202a")

    /*****************************************/
    // 基础数据
    val dataRaw = OcpcCalibrationBase(date, hour, 72, spark).cache()
    dataRaw.show(10)

    // 计费比系数模块
    val jfbDataRaw2 = OcpcJFBfactor(date, hour, expTag, dataRaw, 24, 48, 72, spark)
    val jfbData2 = jfbDataRaw2
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .select("unitid", "conversion_goal", "exp_tag", "jfb_factor")

    jfbData2
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191202b")

  }

}


