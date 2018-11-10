package com.cpc.spark.ocpc.getpb.conversion1and3

import org.apache.spark.sql.{DataFrame, SparkSession}

object getCaliConversion1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val resultDF = getDataV1(date, hour, spark)
    // TODO 删除临时表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_conversion1and3_kvalue_hourly")
  }

  def getDataV1(date: String, hour: String, spark: SparkSession)  = {
    // TODO 将conversion1的逻辑移到这里
    val kValue = spark.table("test.ocpc_k_value_table")
    kValue
  }
}