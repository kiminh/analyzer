package com.cpc.spark.ocpcV3.ocpcNovel.result

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object OcpcGetCpa {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // TODO 测试
    val result = getCPAhistory(date, hour, spark)
  }

  def getCPAhistory(date: String, hour: String, spark: SparkSession) = {
    val tableName = "test.ocpcv3_novel_cpa_history_hourly"
    val rawData = spark
      .table(tableName)
      .where(s"`date`='$date' and `hour`='$hour'")
    rawData.show(10)

    rawData
  }
}