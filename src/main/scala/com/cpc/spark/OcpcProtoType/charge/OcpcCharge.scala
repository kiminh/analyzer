package com.cpc.spark.OcpcProtoType.charge

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcCharge {
  def main(args: Array[String]): Unit = {
    /*
    根据最近四天有投放oCPC广告的广告单元各自的消费时间段的消费数据统计是否超成本和赔付数据
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString

    val ocpcOpenTime = getOcpcOpenTime(date, hour, spark)

  }

  def getOcpcOpenTime(date: String, hour: String, spark: SparkSession) = {
    /*
    从dl_cpc.ocpc_unit_list_hourly抽取每个单元最后一次打开oCPC的时间
     */
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    conversion_goal,
         |    ocpc_last_open_date,
         |    ocpc_last_open_hour
         |FROM
         |
       """.stripMargin
  }
}