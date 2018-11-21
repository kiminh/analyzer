package com.cpc.spark.ocpc.pid

import com.cpc.spark.ocpc.OcpcPIDwithCPA._
import org.apache.spark.sql.SparkSession

object OcpcPID {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
  }

  def calculateKV1(date: String, hour: String, spark: SparkSession) = {
    /**
      * 基于前24个小时的平均k值和那段时间的cpa_ratio，按照更加详细的分段函数对k值进行计算
      * 不考虑复杂的权重，仅按之间计算权重
      */
    val baseData = getBaseTable(date, hour, spark)
    println("################ baseData #################")
    val historyData = getHistoryData(date, hour, 24, spark)
    println("################# historyData ####################")
    val avgK = getAvgK(baseData, historyData, date, hour, spark)
    println("################# avgK table #####################")
    val cpaRatio = getCPAratioV3(baseData, historyData, date, hour, spark)
    println("################# cpaRatio table #######################")
    val newK = updateKv2(baseData, avgK, cpaRatio, date, hour, spark)
    println("################# final result ####################")
    newK
  }
}