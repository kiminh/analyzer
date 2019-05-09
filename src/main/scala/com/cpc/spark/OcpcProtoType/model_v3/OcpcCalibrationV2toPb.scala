package com.cpc.spark.OcpcProtoType.model_v3

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import ocpcFactor.ocpcFactor.{OcpcFactorList, SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object OcpcCalibrationV2toPb {
  def main(args: Array[String]): Unit = {
    /*
    val expTag: Nothing = 1
    val unitid: Nothing = 2
    val ideaid: Nothing = 3
    val slotid: Nothing = 4
    val slottype: Nothing = 5
    val adtype: Nothing = 6
    val cvrCalFactor: Double = 7
    val jfbFactor: Double = 8
    val postCvr: Double = 9
    val highBidFactor: Double = 10
    val lowBidFactor: Double = 11
    计算新版的cvr平滑策略：
    1. 抽取基础数据
    2. 计算该维度下pcoc与计费比、后验cvr等等指标
    3. 计算该维度下根据给定highBidFactor计算出的lowBidFactor
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val fileName = "test.pb"

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version")

    // 抽取基础数据


  }

}
