package com.cpc.spark.oCPX.unittest

import com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_calibration.OcpcCalculateHourlyCali_weightv1.{OcpcCalibrationBase, OcpcCurrentPcoc, OcpcPrevPcoc}
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
    val version = "ocpctest"
    val expTag = "weightv1"
    val hourInt = 89

    println("parameters:")
    println(s"date=$date, hour=$hour")

    // base data
    val dataRaw = OcpcCalibrationBase(date, hour, hourInt, spark).cache()
    dataRaw.show(10)

    // cvr_factor based on previous pcoc
    val previousPcoc = OcpcPrevPcoc(dataRaw, expTag, spark)

    // current pcoc
    val currentPcoc = OcpcCurrentPcoc(dataRaw, expTag, spark)

    // data join
    val data = currentPcoc
      .join(previousPcoc, Seq("unitid", "conversion_goal", "exp_tag"), "inner")
      .select("unitid", "conversion_goal", "exp_tag", "pcoc", "current_pcoc")

    val resultDF = data
      .select("unitid", "conversion_goal", "pcoc", "current_pcoc")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .withColumn("exp_tag", col("exp_tag"))

    resultDF
      .write.mode("overwrite").saveAsTable("test.check_ocpc_unit_test20191224c")


  }

}


