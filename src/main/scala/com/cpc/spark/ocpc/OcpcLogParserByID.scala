package com.cpc.spark.ocpc

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col


object OcpcLogParserByID {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcMonitor").enableHiveSupport().getOrCreate()

    val day = args(0).toString
    val ideaid = args(1).toString

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  ocpc_log,
         |  isshow,
         |  isclick,
         |  iscvr
         |FROM
         |  dl_cpc.ocpc_result_unionlog_table_bak
         |WHERE
         |  `date`='$day'
         |AND
         |  ideaid='$ideaid'
       """.stripMargin

    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    logParser(data)
  }

  def logParser(dataset: DataFrame): Unit = {
    val newData = dataset.withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    newData.write.mode("overwrite").saveAsTable("test.ocpc_log_parser_20181030")
  }
}
