package com.cpc.spark.OcpcProtoType.report_qtt

import com.cpc.spark.tools.OperateMySQL
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.report.OcpcHourlyReport._
import org.apache.log4j.{Level, Logger}

object OcpcHourlyReportV2 {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    /*
    新版报表程序
    1. 从ocpc_unionlog拉取ocpc广告记录
    2. 采用数据关联方式获取转化数据
    3. 统计分ideaid级别相关数据
    4. 统计分conversion_goal级别相关数据
    5. 存储到hdfs
    6. 存储到mysql
     */
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media")

    // 拉取点击、消费、转化等基础数据
    var isHidden = 0
    if (version == "qtt_demo") {
      isHidden = 0
    } else {
      isHidden = 1
    }
    val baseData = getBaseData(media, date, hour, spark).filter(s"is_hidden = $isHidden")

    // 分ideaid和conversion_goal统计数据
    val rawDataUnit = preprocessDataByUnit(baseData, date, hour, spark)
    val dataUnit = getDataByUnit(rawDataUnit, version, date, hour, spark)

    // 分conversion_goal统计数据
    val rawDataConversion = preprocessDataByConversion(dataUnit, date, hour, spark)
    val costDataConversion = preprocessCostByConversion(dataUnit, date, hour, spark)
    val dataConversion = getDataByConversion(rawDataConversion, version, costDataConversion, date, hour, spark)

    // 存储数据到hadoop
    saveDataToHDFS(dataUnit, dataConversion, version, date, hour, spark)



  }



}