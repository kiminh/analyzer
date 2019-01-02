package com.cpc.spark.ocpcV3.ocpc.data

import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcCPArecommend {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession
      .builder()
      .appName(s"ocpc cpa recommend: $date, $hour")
      .enableHiveSupport().getOrCreate()

    // 根据is_ocpc和length(ocpc_log)<=0来确定

    // 计算cpc阶段广告的实际cpa
//    val rawData = getCPCstageAd(date, hour, spark)
//    val resultDF = calculateCPArecommend(rawData, date, hour, spark)
//
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_cpa_recommend_hourly")

  }

  def getCPCstageAd(date: String, hour: String, spark: SparkSession) = {

  }

  def calculateCPArecommend(data: DataFrame, date: String, hour: String, spark: SparkSession) = {

  }
}