package com.cpc.spark.ml.calibration

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils
import org.apache.spark.sql.functions._


object HourlyCalibrationOnMiduCvr {

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

    val timeRangeSql = Utils.getTimeRangeSql_3(startDate, startHour, endDate, endHour)
    val timeRangeSql2 = Utils.getTimeRangeSql(startDate, startHour, endDate, endHour)

    // get union log
    val sql = s"""
                 |select iscvr, cast(raw_cvr as bigint) as ectr, 0 as show_timestamp, cvr_model_name from
                 |(select *
                 | from dl_cpc.slim_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80001098', '80001292') and isshow = 1 and cvr_model_name <>''
                 | and ctr_model_name != 'noctr'
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND (charge_type IS NULL OR charge_type = 1))a
                 | left join
                 |(select searchid, label2 as iscvr from dl_cpc.ml_cvr_feature_v1
                 |WHERE $timeRangeSql2
                 |) b on a.searchid = b.searchid
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql).select("isclick","ecvr","show_timestamp","cvr_model_name")
        .withColumn("isclick",when(col("isclick")===null,0).otherwise(col("isclick")))
        .select("isclick","ecvr","show_timestamp","cvr_model_name")


    HourlyCalibration.unionLogToConfig(log.rdd, session.sparkContext, softMode)
  }
}