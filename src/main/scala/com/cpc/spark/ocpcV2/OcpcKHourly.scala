package com.cpc.spark.ocpcV2

import org.apache.spark.sql.SparkSession

object OcpcKHourly {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("ocpc v2").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    //
  }
}