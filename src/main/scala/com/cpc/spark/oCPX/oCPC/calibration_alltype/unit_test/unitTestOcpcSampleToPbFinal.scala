package com.cpc.spark.oCPX.oCPC.calibration_alltype.unit_test

import com.cpc.spark.oCPX.oCPC.calibration_alltype.OcpcSampleToPbFinal._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object unitTestOcpcSampleToPbFinal {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // bash: 2019-01-02 12 qtt_demo 1
    val date = args(0).toString
    val hour = args(1).toString

    val data = getRangeValue(date, hour, spark)
    data
      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data_unit_test20191026")

  }


}

