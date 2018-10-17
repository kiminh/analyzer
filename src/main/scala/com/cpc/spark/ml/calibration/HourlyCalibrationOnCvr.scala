package com.cpc.spark.ml.calibration

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils


object HourlyCalibrationOnCvr {

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

    val timeRangeSql = Utils.getTimeRangeSql(startDate, startHour, endDate, endHour)

    // get union log
    val sql = s"""
                 |select if(iscvr is null, 0, iscvr), ecvr, time, model_name from
                 |( select searchid, ext_int['raw_cvr'] as ecvr, show_timestamp as time, ext_string['cvr_model_name'] as model_name
                 | from dl_cpc.cpc_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80000001', '80000002')
                 | and isclick = 1
                 | and ext['antispam'].int_value = 0
                 | and ideaid > 0
                 | and adsrc = 1
                 | and adslot_type in (1, 2, 3)
                 | and userid > 0
                 | and (ext["charge_type"] IS NULL
                 |       OR ext["charge_type"].int_value = 1)
                 | and round(ext["adclass"].int_value/1000) != 132101
                 | )
                 | a left join
                 |(select searchid, label as iscvr from dl_cpc.ml_cvr_feature_v1
                 |WHERE $timeRangeSql
                 |) b on a.searchid = b.searchid
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql)

    HourlyCalibration.unionLogToConfig(log.rdd, session.sparkContext, softMode)
  }
}