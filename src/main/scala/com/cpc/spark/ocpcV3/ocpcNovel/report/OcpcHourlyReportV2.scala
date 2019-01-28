package com.cpc.spark.ocpcV3.ocpcNovel.report

import org.apache.spark.sql.SparkSession


object OcpcHourlyReportV2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReportV2: novel")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
  }
}