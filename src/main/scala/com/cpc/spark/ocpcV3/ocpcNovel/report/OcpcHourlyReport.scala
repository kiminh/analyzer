package com.cpc.spark.ocpcV3.ocpcNovel.report

import org.apache.spark.sql.SparkSession

object OcpcHourlyReport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
  }


}