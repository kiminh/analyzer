package com.cpc.spark.oCPX.unittest

import com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction.prepareLabel.prepareLabelMain
import com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction.v3.prepareTrainingSample.{getFeatureData, udfAddHour}
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

    val baseDataRaw = prepareLabelMain(date, hour, hourInt, spark)

    val data = baseDataRaw
      .withColumn("time", concat_ws(" ", col("date"), col("hour")))
      .withColumn("label", col("pcoc"))
      .filter("label is not null")
      .select("identifier", "media", "conversion_goal", "conversion_from", "label", "time", "date", "hour")

    data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191129b")
//    data
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191129a")

  }

}


