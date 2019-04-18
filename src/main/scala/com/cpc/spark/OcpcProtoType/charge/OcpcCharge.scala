package com.cpc.spark.OcpcProtoType.charge

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcCharge {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString

    val ocpcOpenTime = getOcpcOpenTime(date, hour, spark)

  }

  def getOcpcOpenTime(date: String, hour: String, spark: SparkSession) = {

  }
}