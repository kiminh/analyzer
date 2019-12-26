package com.cpc.spark.oCPX.unittest


import com.cpc.spark.oCPX.deepOcpc.calibration_v6.OcpcGetPb_retention.{OcpcCalibrationBase, calculateDataCvr, calculateDeepCvr}
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
//    val version = "ocpctest"
//    val expTag = "weightv1"
//    val hourInt = 89

    println("parameters:")
    println(s"date=$date, hour=$hour")


    // base data
    val dataRaw = OcpcCalibrationBase(date, hour, 72, spark)

    // calculate post_cvr1, pre_cvr2
    val data1 = calculateDataCvr(dataRaw, 80, spark)

    // calculate deep_cvr
    val data2 = calculateDeepCvr(date, 3, spark)

    // data join
    val data = data1
      .join(data2, Seq("unitid", "media"), "inner")

    val result = data
      .withColumn("cvr_factor", col("post_cvr1") * col("deep_cvr") * 1.0 / col("pre_cvr2"))

    result
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data201901226c")


  }

}


