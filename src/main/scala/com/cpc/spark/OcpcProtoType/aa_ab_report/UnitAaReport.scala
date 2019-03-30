package com.cpc.spark.OcpcProtoType.aa_ab_report

import org.apache.spark.sql.SparkSession
import com.cpc.spark.OcpcProtoType.aa_ab_report.GetBaseData
object UnitAaReport {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession.builder().appName("UnitAaReport").enableHiveSupport().getOrCreate()
    //GetBaseData.getBaseData(date, hour, spark)
    GetBaseData.getBaseData(date, hour, spark)
    println("inser data success")
  }
}
