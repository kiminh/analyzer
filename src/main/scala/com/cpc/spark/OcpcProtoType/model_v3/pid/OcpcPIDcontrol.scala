package com.cpc.spark.OcpcProtoType.model_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcPIDcontrol {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    val version = args(3).toString
    val sampleHour = args(4).toInt
    val kp = args(5).toDouble
    val ki = args(6).toDouble
    val kd = args(7).toDouble

    val baseData = getBaseData(media, sampleHour, date, hour, spark)
  }

  def getBaseData(media: String, sampleHour: Int, date: String, hour: String, spark: SparkSession) = {
    
  }
}

