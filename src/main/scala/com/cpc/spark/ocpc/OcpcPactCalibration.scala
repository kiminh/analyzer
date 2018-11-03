package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession

object OcpcPactCalibration {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // calculate time period for historical data
    val date = args(0).toString
    val hour = args(1).toString
  }

  def calibrationV1(date: String, hour: String, spark: SparkSession) = {

  }
}