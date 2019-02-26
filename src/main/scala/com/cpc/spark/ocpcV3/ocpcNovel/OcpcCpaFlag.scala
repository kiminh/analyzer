package com.cpc.spark.ocpcV3.ocpcNovel

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import ocpcnovel.ocpcnovel.{OcpcNovelList, SingleUnit}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
//import ocpcnovel.ocpcnovel

import scala.collection.mutable.ListBuffer


object OcpcCpaFlag {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString

    val CPAFlag =
      s"""
        |select
        | identifier,
        | case when cpa_ratio < 0.64 and cost >100000 then '1'
        | else '0' end as flag
        | from dl_cpc.ocpc_detail_report_hourly_v3
        | where `date`= ${getDay(date, 1)} and `hour`= '23'
      """.stripMargin


  }
  def getDay(startdate: String, day: Int): String = {
    val format = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(startdate))
    cal.add(Calendar.DATE, -day)
    format.format(cal.getTime)
  }
}
