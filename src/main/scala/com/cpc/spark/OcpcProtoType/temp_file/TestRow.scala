package com.cpc.spark.OcpcProtoType.temp_file

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row

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
    val row = Row("1", 2, "3")
    val dataList = spark.sql(sql).collect()
    for(list <- dataList){
      for(i <- 0 until list.length) print(list.get(i) + "\t")
      println()
    }
  }
}
