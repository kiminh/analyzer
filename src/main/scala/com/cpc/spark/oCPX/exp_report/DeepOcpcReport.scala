package com.cpc.spark.oCPX.exp_report

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object DeepOcpcReport {
  def main(args: Array[String]): Unit = {
    /*
    次留实验报表数据
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // bash: 2019-01-02 12
    val date = args(0).toString
    val dayInt = args(1).toInt



  }

  def getCompleteExp(date: String, dayInt: Int, spark: SparkSession) = {

  }

}