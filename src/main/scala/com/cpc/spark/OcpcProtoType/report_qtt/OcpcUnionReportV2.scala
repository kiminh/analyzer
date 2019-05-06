package com.cpc.spark.OcpcProtoType.report_qtt

import com.cpc.spark.tools.OperateMySQL
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.report.OcpcUnionReport._
import org.apache.log4j.{Level, Logger}

object OcpcUnionReportV2 {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession.builder().appName("OcpcUnionAucReport").enableHiveSupport().getOrCreate()
    // get the unit data
    val dataUnitRaw = unionDetailReport(date, hour, spark)
    // get the suggest cpa
    val dataUnit = addSuggestCPA(dataUnitRaw, date, hour, spark)
    println("------union detail report success---------")
    val dataConversion = unionSummaryReport(date, hour, spark)
    println("------union summary report success---------")
    saveDataToMysql(dataUnit, dataConversion, date, hour, spark)
    println("------insert into mysql success----------")
  }

}
