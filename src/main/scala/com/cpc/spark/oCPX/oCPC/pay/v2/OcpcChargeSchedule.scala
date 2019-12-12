package com.cpc.spark.oCPX.oCPC.pay.v2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfDetermineMedia}
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
    val ocpcCompensate = getOcpcCompensate(date, dayCnt, spark)

    // 统计今天的分单元消耗和开始消费时间
    val todayData = getTodayData(date, spark)

    // 关联赔付周期表
    val data = joinSchedule(ocpcCompensate, todayData, spark)

    // 更新赔付周期表
    val result = updateSchedule(data, date, dayCnt, spark)

  }

  def updateSchedule(dataRaw: DataFrame, date: String, dayCnt: Int, spark: SparkSession) = {
    val data = dataRaw
      .withColumn("pay_schedule", udfCheckDate(date, dayCnt)(col("ocpc_charge_time")))
      .withColumn("pay_cnt", col("pay_schedule").getItem(0))
      .withColumn("calc_dates", col("pay_schedule").getItem(1))
      .withColumn("flag", udfDeterminePayFlag()(col("pay_cnt"), col("deep_ocpc_charge_time")))


  }

  def udfDeterminePayFlag() = udf((payCnt: Int, deepOcpcChargeTime: String) => {
    val result = {
      if (payCnt >= 4 && deepOcpcChargeTime == " ") {
        0
      } else {
        1
      }
    }
    result
  })

  def udfCheckDate(date: String, dayCnt: Int) = udf((ocpcChargeTime: String) => {
    // 取历史数据
    val dateConverter1 = new SimpleDateFormat("yyyy-MM-dd")
    val dateConverter2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:SS")

    val today = dateConverter1.parse(date)
    val todayCalendar = Calendar.getInstance()
    todayCalendar.setTime(today)

    val ocpcChargeDate = dateConverter2.parse(ocpcChargeTime)
    val ocpcChargeDateCalendar = Calendar.getInstance()
    ocpcChargeDateCalendar.setTime(ocpcChargeDate)
    val dateDiff = todayCalendar.get(Calendar.DATE) - ocpcChargeDateCalendar.get(Calendar.DATE) + 1
    val payCnt = dateDiff / dayCnt
    val calcDates = dateDiff % dayCnt

    val result = Array(payCnt, calcDates)
    result
  })

  def joinSchedule(ocpcCompensate: DataFrame, todayData: DataFrame, spark: SparkSession) = {
    /*
    关联周期表与今天的新数据
    1. 过滤ocpcCompensate表中ocpc_charge_time中的第一条记录，记为表1
    2. 过滤ocpcCompensate表中final_charge_time中最后一条记录，记为表2
    3. 表1与表2外关联，记为表3
    4. 令todayData为表4
    5. 表3与表4外关联，记为表5
     */
    ocpcCompensate.createOrReplaceTempView("ocpc_compensate")

    // 过滤ocpcCompensate表中ocpc_charge_time中的第一条记录，记为表1
    // 过滤ocpcCompensate表中final_charge_time中最后一条记录，记为表2
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  ocpc_charge_time as first_charge_time,
         |  ocpc_charge_time as last_ocpc_charge_time,
         |  deep_ocpc_charge_time as last_deep_ocpc_charge_time,
         |  row_number() over(partition by unitid order by ocpc_charge_time) as seq1,
         |  row_number() over(partition by unitid order by final_charge_time desc) as seq2
         |FROM
         |  ocpc_compensate
         |WHERE
         |  ocpc_charge_time != ' '
         |""".stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .filter(s"seq1 = 1")
      .select("unitid", "first_charge_time")
      .distinct()

    val data2 = spark
      .sql(sqlRequest1)
      .filter(s"seq2 = 1")
      .select("unitid", "last_ocpc_charge_time", "last_deep_ocpc_charge_time")
      .distinct()

    // 表1与表2外关联，记为表3
    val data3 = data1.join(data2, Seq("unitid"), "outer")

    // 令todayData为表4
    val data4 = todayData
      .select("unitid", "ocpc_charge_time", "deep_ocpc_charge_time")

    // 表3与表4外关联，记为表5
    val data5 = data3
      .join(data4, Seq("unitid"), "outer")
      .select("unitid", "ocpc_charge_time", "deep_ocpc_charge_time", "first_charge_time", "last_ocpc_charge_time", "last_deep_ocpc_charge_time")

    data5
  }

  def joinScheduleV2(ocpcCompensate: DataFrame, todayData: DataFrame, spark: SparkSession) = {
    /*
    1. 过滤出ocpcCompensate表中ocpc_charge_time不为空的记录，保留一条，记为表1
    2. 过滤出ocpcCompensate表中deep_ocpc_charge_time不为空的记录，保留一条，记为表2
    3. 令todayData为表3
    4. 令todayData中deep_ocpc_charge_time不为空的为表4
    5. 表1与表3关联，获取不在表1在表3的记录，记为表5
    6. 表2与表4关联，获取不在表2在表4的记录，记为表6
    7. ocpcCompensate表中的ocpc_charge_time保留最后一条，如果超过赔付周期或者赔付次数超过上限，则不再赔付，表7
    8. ocpcCompensate表中的deep_ocpc_charge_time保留最后一条，表8
    9. 表7与表5进行union， 表9
    10. 表8月表6进行union， 表10
    11. 表9与表10外关联，表11
    12. ocpcCompensate中取出unitid与pay_cnt，按照final_ocpc_charge_time最后一条记录，表12
    13. 表11关联表12，pay_cnt填0
     */

    // 过滤出ocpcCompensate表中ocpc_charge_time不为空的记录，保留一条，记为表1
    val data1 = ocpcCompensate
      .withColumn("flag", lit(1))
      .filter(s"ocpc_charge_time != ' '")
      .select("unitid", "flag")
      .distinct()

    // 过滤出ocpcCompensate表中deep_ocpc_charge_time不为空的记录，保留一条，记为表2
    val data2 = ocpcCompensate
      .filter(s"deep_ocpc_charge_time != ' '")
      .withColumn("flag", lit(1))
      .select("unitid", "flag")
      .distinct()

    // 令todayData为表3
    val data3 = todayData
      .select("unitid", "ocpc_charge_time")
      .distinct()

    // 令todayData中deep_ocpc_charge_time不为空的为表4
    val data4 = todayData
      .filter(s"deep_ocpc_charge_time != ' '")
      .select("unitid", "deep_ocpc_charge_time")
      .distinct()

    // 表1与表3关联，获取不在表1在表3的记录，记为表5
    val data5 = data3
      .join(data1, Seq("unitid"), "left_outer")
      .filter(s"flag is null")
      .select("unitid", "ocpc_charge_time")
      .distinct()

    // 表2与表4关联，获取不在表2在表4的记录，记为表6
    val data6 = data4
      .join(data2, Seq("unitid"), "left_outer")
      .filter(s"flag is null")
      .select("unitid", "deep_ocpc_charge_time")
      .distinct()

    // ocpcCompensate表中的ocpc_charge_time保留最后一条，如果超过赔付周期或者赔付次数超过上限，则不再赔付，表7
    ocpcCompensate.createOrReplaceTempView("ocpc_compensate")
    val sqlRequest7 =
      s"""
         |SELECT
         |  unitid,
         |  ocpc_charge_time,
         |  row_number() over(partition by unitid order by ocpc_charge_time desc) as seq1,
         |  row_number() over(partition by
         |FROM
         |  ocpc_compensate
         |WHERE
         |  ocpc_charge_time != ' '
         |""".stripMargin
    println(sqlRequest7)
    val data7 = spark
      .sql(sqlRequest7)
      .filter(s"seq = 1")
      .select("unitid", "ocpc_charge_time")
      .distinct()

    // ocpcCompensate表中的deep_ocpc_charge_time保留最后一条，表8
    val sqlRequest8 =
      s"""
         |SELECT
         |  unitid,
         |  deep_ocpc_charge_time,
         |  row_number() over(partition by unitid order by deep_ocpc_charge_time desc) as seq
         |FROM
         |  ocpc_compensate
         |WHERE
         |  deep_ocpc_charge_time != ' '
         |""".stripMargin
    val data8 = spark
      .sql(sqlRequest8)
      .filter(s"seq = 1")
      .select("unitid", "deep_ocpc_charge_time")
      .distinct()

    // 表7与表5进行union， 表9
    val data9 = data5.union(data7)

    // 表8月表6进行union， 表10
    val data10 = data6.union(data8)

    // 表9与表10外关联，表11
    val data11 = data9
      .join(data10, Seq("unitid"), "outer")
      .select("unitid", "ocpc_charge_time", "deep_ocpc_charge_time")

    // ocpcCompensate中取出unitid与pay_cnt，按照final_ocpc_charge_time最后一条记录，表12
    val sqlRequest12 =
      s"""
         |SELECT
         |  unitid,
         |  pay_cnt,
         |  row_number() over(partition by unitid order by final_charge_time desc) as seq
         |FROM
         |  ocpc_compensate
         |WHERE
         |  final_charge_time != ' '
         |""".stripMargin
    println(sqlRequest12)
    val data12 = spark
      .sql(sqlRequest12)
      .filter(s"seq = 1")
      .select("unitid", "pay_cnt")
      .distinct()

    // 数据关联，表13
    val data13 = data11
      .join(data12, Seq("unitid"), "left_outer")
      .select("unitid", "ocpc_charge_time", "deep_ocpc_charge_time", "pay_cnt")
      .na.fill(" ", Seq("ocpc_charge_time", "deep_ocpc_charge_time"))
      .na.fill(0, Seq("pay_cnt")) // pay_cnt代表已完成的赔付周期数

    data13
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
      .select("unitid", "timestamp", "ocpc_charge_time")

    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  timestamp as deep_timestamp,
         |  from_unixtime(timestamp,'YYYY-MM-dd HH:mm:ss') as deep_ocpc_charge_time,
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
      .select("searchid", "unitid", "deep_timestamp", "deep_ocpc_charge_time")

    val data = data1
      .join(data2, Seq("unitid"), "left_outer")
      .select("unitid", "timestamp", "ocpc_charge_time", "deep_timestamp", "deep_ocpc_charge_time")

    data
  }


  def getOcpcCompensate(date: String, dayCnt: Int, spark: SparkSession) = {
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
    val data = spark
        .sql(sqlRequest2)
        .filter(s"ocpc_charge_time is not null")

    data
  }

}
