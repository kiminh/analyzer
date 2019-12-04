package com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction.v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcMaeMonitor {
  def main(args: Array[String]): Unit = {
    /*
    采用点击加权mae评估前12小时的baseline和pcoc预估校准策略
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(3).toString
    val expTag1 = args(4).toString
    val expTag2 = args(5).toString
    val hourDiff = args(6).toInt


    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, expTag1=$expTag1, expTag2=$expTag2, hourDiff=$hourDiff")

    // baseline校准策略的pcoc
    val baselineData = getBaselinePcoc(date, hour, 12, hourDiff, version, expTag2, spark)

    // pcoc预估校准策略的pcoc

    // 真实pcoc

    // 计算分小时分单元分媒体mae

    // 计算点击加权分单元分媒体mae


  }

  def getBaselinePcoc(date: String, hour: String, hourInt: Int, hourDiff: Int, version: String, expTag: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    calendar.add(Calendar.HOUR, -hourDiff)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |   unitid,
         |   exp_tag,
         |   1.0 / cvr_factor as pcoc,
         |   date,
         |   hour
         |FROM
         |    dl_cpc.ocpc_pb_data_hourly_exp
         |WHERE
         |    $selectCondition
         |AND
         |    version = '$version'
         |AND
         |    exp_tag like '${expTag}%'
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }



}


