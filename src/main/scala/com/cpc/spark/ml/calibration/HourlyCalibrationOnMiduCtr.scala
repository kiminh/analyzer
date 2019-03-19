package com.cpc.spark.ml.calibration

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import com.cpc.spark.common.Utils
import org.apache.spark.sql.functions._


object HourlyCalibrationOnMiduCtr {

  val localDir = "/home/cpc/scheduled_job/hourly_calibration/"
  val destDir = "/home/work/mlcpp/calibration/"
  val MAX_BIN_COUNT = 20
  val MIN_BIN_SIZE = 10000

  def main(args: Array[String]): Unit = {

    // parse and process input
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val softMode = args(3).toInt


    val endTime = LocalDateTime.parse(s"$endDate-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))

    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    println(s"endDate=$endDate")
    println(s"endHour=$endHour")
    println(s"hourRange=$hourRange")
    println(s"startDate=$startDate")
    println(s"startHour=$startHour")
    println(s"softMode=$softMode")

    // build spark session
    val session = Utils.buildSparkSession("hourlyCalibration")

    val timeRangeSql = getTimeRangeSql(startDate, startHour, endDate, endHour)

    // get union log
    val sql = s"""
                 |select isclick, cast(raw_ctr as bigint) as ectr, show_timestamp, ctr_model_name from dl_cpc.cpc_basedata_union_events
                 | where $timeRangeSql
                 | and media_appsid in ('80001098', '80001292') and isshow = 1 and ctr_model_name <>''
                 | and ctr_model_name != 'noctr'
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql).select("isclick","ectr","show_timestamp","ctr_model_name")
        .withColumn("isclick",when(col("isclick")===null,0).otherwise(col("isclick")))
        .select("isclick","ectr","show_timestamp","ctr_model_name")
        .sample(false,0.5)

    HourlyCalibration.unionLogToConfig(log.rdd, session.sparkContext, softMode)
  }

  def getTimeRangeSql(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(day = '$startDate' and hour <= '$endHour' and hour >= '$startHour')"
    }
    return s"((day = '$startDate' and hour >= '$startHour') " +
      s"or (day = '$endDate' and hour <= '$endHour') " +
      s"or (day > '$startDate' and day < '$endDate'))"
  }
}