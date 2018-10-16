package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession

object OcpcMonitor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcMonitor").enableHiveSupport().getOrCreate()

    val day = args(0).toString
    val hr = args(1).toString

    val sqlRequest =
      s"""
         |SELECT
         |
       """.stripMargin
  }
}
