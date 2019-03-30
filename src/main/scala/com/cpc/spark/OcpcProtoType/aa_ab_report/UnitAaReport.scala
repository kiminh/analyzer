package com.cpc.spark.OcpcProtoType.aa_ab_report

import org.apache.spark.sql.SparkSession

object UnitAaReport {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession.builder().appName("UnitAaReport").enableHiveSupport().getOrCreate()
//    GetBaseData.getBaseData(date, hour, spark)
//    println("inser data success")
    UnitAaReportHourly.getIndexValue(date, hour, spark)
    println("has got index value of aa report hourly")
  }
}
