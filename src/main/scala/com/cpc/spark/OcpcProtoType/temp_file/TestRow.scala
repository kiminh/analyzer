package com.cpc.spark.OcpcProtoType.temp_file

import org.apache.spark.sql.SparkSession

object TestRow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("TestRow").enableHiveSupport().getOrCreate()
    val sql =
      s"""
        |select *
        |from dl_cpc.ocpc_aa_report_daily
        |where `date` = '2019-03-27'
        |limit 5
      """.stripMargin
    val dataList = spark.sql(sql).collectAsList()
    for(list <- dataList){
      for(item <- list) print(item + "\t")
      println()
    }
  }
}
