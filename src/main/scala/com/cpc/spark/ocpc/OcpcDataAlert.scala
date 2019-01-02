package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession


object OcpcDataAlert {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcDataAlert").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    // 如果小时级数据量小于500，则异常退出
    val check1 = alertDataCount(date, hour, spark).toInt
    if (check1<500) {
      println("##################################################")
      println(s"number of records in table dl_cpc.ocpc_result_unionlog_table_bak at $date-$hour: %d".format(check1))
      println("##################################################")
      System.exit(1)
    }

    // TODO 检测其他相关数据累积程序的数据量
  }

  def alertDataCount(date: String, hour: String, spark: SparkSession): Int ={
    // 检测monitor程序获得的小时级数据量，如果小于500则返回true，否则返回false
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_result_unionlog_table_bak
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    val dataCount = data.count().toInt

    dataCount

  }

}
