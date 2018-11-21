package com.cpc.spark.ocpc.getpb.conversion2

import org.apache.spark.sql.SparkSession

object getCaliConversion2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString


  }

  def getDataV1(date: String, hour: String, spark: SparkSession) = {

  }

}