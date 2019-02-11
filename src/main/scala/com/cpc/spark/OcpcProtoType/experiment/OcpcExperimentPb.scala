package com.cpc.spark.OcpcProtoType.experiment

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcExperimentPb {
  def main(args: Array[String]): Unit = {
    /*
    将实验配置数据存储到pb文件中
    1. 按照分隔符|抽取dataSrc，根据配置文件选择数据表
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val dataSrc = args(3).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, dataSrc=$dataSrc")





  }





}


