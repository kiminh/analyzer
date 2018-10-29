package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession


object OcpcDataAlert {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcDataAlert").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    // 如果小时级数据量小于500，则异常退出
//    val check1 = alertDataCount(date, hour, spark)
//    if (check1) {
//      System.exit(1)
//    }
    System.exit(1)
  }

  def alertDataCount(date: String, hour: String, spark: SparkSession):Boolean ={
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
    val data = spark.sql(sqlRequest)
    val dataCount = data.count()

    if (dataCount < 500) {
      true
    } else {
      false
    }

  }

}
