package com.cpc.spark.ocpcV3.ocpc.filter

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils._
import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object OcpcGetCV {
  def main(args: Array[String]): Unit = {
    /*
    每个unitid在每个转化目标下面当天各自累积的转化数据

     */
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toInt
    val version = "qtt_demo"
    val spark = SparkSession
      .builder()
      .appName(s"ocpc get cv: $date, $hour")
      .enableHiveSupport().getOrCreate()




  }
}
