package com.cpc.spark.oCPX.oCPC.pay.v2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.udfDetermineMedia
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcChargeSchedule {
  def main(args: Array[String]): Unit = {
    /*
    ocpc周期控制模块
    结果表包括以下字段：unitid, pay_cnt, pay_date, flag

    pay_cnt: 已经完成赔付的次数
    pay_date: 当前赔付周期的起始日期
    flag: 当前周期是否需要计算赔付(基于pay_cnt判断)

    pay_cnt的更新：
    根据pay_date，date，dayCnt判断该赔付周期是否结束，如果当前周期结束，pay_cnt++

    pay_date的更新：
    更新逻辑同上
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val version = args(1).toString
    val dayCnt = args(2).toInt

    // 获取前一天的ocpc_compensate线上数据（备份表），基于ocpc_charge_time和deep_ocpc_charge_time来判断周期开始日期以及分别需要计算深度还是浅层赔付
    val ocpcCompensate = getOcpcCompensate(date, spark)

    // 统计今天的分单元消耗和开始消费时间
    val data = getTodayData(date, spark)

    // 更新赔付周期表

  }

  def getTodayData(date: String, spark: SparkSession) = {
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  timestamp,
         |  from_unixtime(timestamp,'YYYY-MM-dd HH:mm:ss') as ocpc_charge_time,
         |  row_number() over(partition by unitid order by timestamp) as seq
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  date = '$date'
         |""".stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .filter(s"seq = 1")
      .withColumn("is_deep_ocpc", lit(0))
      .select("searchid", "unitid", "time_stamp", "ocpc_charge_time")

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  timestamp,
         |  from_unixtime(timestamp,'YYYY-MM-dd HH:mm:ss') as ocpc_charge_time,
         |  row_number() over(partition by unitid order by timestamp) as seq
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  date = '$date'
         |AND
         |  deep_ocpc_step = 2
         |""".stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .filter(s"seq = 1")
      .withColumn("is_deep_ocpc", lit(1))
      .select("searchid", "unitid", "time_stamp", "ocpc_charge_time")

    val data = data1.union(data2)

    data
  }


  def getOcpcCompensate(date: String, spark: SparkSession) = {
    // ocpc赔付备份表
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  ocpc_charge_time,
         |  deep_ocpc_charge_time,
         |  compensate_key
         |FROM
         |  dl_cpc.ocpc_compensate_backup_daily
         |WHERE
         |  `date` = '$date'
         |""".stripMargin
    println(sqlRequest1)
    val dataRaw = spark.sql(sqlRequest1)
    dataRaw.createOrReplaceTempView("raw_data")

    // 整合ocpc_charge_time
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  ocpc_charge_time,
         |  deep_ocpc_charge_time,
         |  compensate_key,
         |  (case when ocpc_charge_time == " " then deep_ocpc_charge_time
         |        else ocpc_charge_time
         |   end) as final_charge_time,
         |   cast(split(compensate_key, '~')[1] as int) as pay_cnt
         |FROM
         |  raw_data
         |""".stripMargin
    println(sqlRequest2)
    val data = spark.sql(sqlRequest2)

    data
  }



}
