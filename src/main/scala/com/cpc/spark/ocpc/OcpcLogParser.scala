package com.cpc.spark.ocpc

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.functions._

object OcpcLogParser {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcMonitor").enableHiveSupport().getOrCreate()

    val day = args(0).toString
    val hour = args(1).toString

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  ocpc_log
         |FROM
         |  dl_cpc.ocpc_result_unionlog_table_bak
         |WHERE
         |  `date`='$day'
         |AND
         |  `hour`='$hour'
       """.stripMargin

    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    logParser(data)
  }

  def logParser(dataset: DataFrame): Unit = {
    val newData = dataset.withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

//    newData.write.mode("overwrite").saveAsTable("test.ocpc_log_parser_20181025")
    newData
  }
}
