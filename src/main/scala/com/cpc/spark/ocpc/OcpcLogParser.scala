package com.cpc.spark.ocpc

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.functions._

object OcpcLogParser {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcMonitor").enableHiveSupport().getOrCreate()

    val tableName = args(0).toString

    val result = logParser(tableName, spark)
    result.write.mode("overwrite").saveAsTable("test.ocpc_log_parsed_table")
  }

  def logParser(tableName: String, spark: SparkSession): DataFrame = {
    val rawData = spark.table(tableName)
    val resultDF = rawData.
      withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    resultDF.show(10)
    resultDF
  }
}
