package com.cpc.spark.ocpc.savepb

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, when}
import com.cpc.spark.ocpc.utils.OcpcUtils._

object getCompletePb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

  }

  def saveFinalDataIntoPb(date: String, hour: String, spark: SparkSession) = {

  }
}
