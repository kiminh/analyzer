package com.cpc.spark.OcpcProtoType.model_v5


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcGetPb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val highBidFactor = args(4).toDouble
    val lowBidFactor = args(5).toDouble
    val hourInt = args(6).toInt
    val conversionGoal = args(7).toInt
    val minCV = args(8).toInt

    // 主校准回溯时间长度
    val hourInt1 = args(9).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(10).toInt
    // 兜底校准时长
    val hourInt3 = args(11).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, media:$media, highBidFactor:$highBidFactor, lowBidFactor:$lowBidFactor, hourInt:$hourInt, conversionGoal:$conversionGoal, minCV:$minCV, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    val calibraionData = OcpcCalculateCalibration.OcpcCalculateCalibration(date, hour, conversionGoal, version, media, minCV, hourInt1, hourInt2, hourInt3, spark).cache()
    calibraionData.show(10)
    val factorData = OcpcRangeCalibration.OcpcRangeCalibration(date, hour, version, media, highBidFactor, lowBidFactor, hourInt, conversionGoal, minCV, spark).cache()
    factorData.show(10)



  }



}


