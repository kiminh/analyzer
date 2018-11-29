package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcUtils {
  def getTimeRangeSql2(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`date` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`date` = '$startDate' and hour > '$startHour') " +
      s"or (`date` = '$endDate' and hour <= '$endHour') " +
      s"or (`date` > '$startDate' and `date` < '$endDate'))"
  }

  def getTimeRangeSql3(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`dt` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`dt` = '$startDate' and hour > '$startHour') " +
      s"or (`dt` = '$endDate' and hour <= '$endHour') " +
      s"or (`dt` > '$startDate' and `dt` < '$endDate'))"
  }

  def getHistoryData(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcMonitor程序的结果表获取历史数据
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  adclass,
         |  isshow,
         |  isclick,
         |  iscvr,
         |  price,
         |  ocpc_log,
         |  hour
         |FROM
         |  dl_cpc.ocpc_result_unionlog_table_bak
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }


  def getCompleteData(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcMonitor程序的结果表获取历史数据
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  ctr_cnt,
         |  cvr_cnt,
         |  hour
         |FROM
         |  dl_cpc.ocpc_uid_userid_track_label2
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getPcvrData(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcAccPcvr程序的结果表获取历史数据
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  cnt,
         |  total_cvr,
         |  hour
         |FROM
         |  dl_cpc.ocpc_pcvr_history
         |WHERE $selectCondition
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getActData(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcActivationData程序的结果表获取历史数据
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  cost,
         |  ctr_cnt,
         |  cvr_cnt,
         |  hour
         |FROM
         |  dl_cpc.ocpc_ideaid_adclass_label3_track_v1
         |WHERE $selectCondition
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getActDataNovel(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcActivationData程序的结果表获取历史数据
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  hour,
         |  sum(cvr2_cnt) as cvr2cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr2_data_hourly
         |WHERE
         |  media_appsid in ("80001098", "80001292")
         |  and $selectCondition
         |group by unitid,adclass, hour
       """.stripMargin
    println(sqlRequest)
    val cvrData = spark.sql(sqlRequest)

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  sum(total_price) as cost,
         |  hour
         |FROM
         |  dl_cpc.ocpcv3_ctr_data_hourly
         |WHERE
         |  $selectCondition
         |and
         |  media_appsid in ("80001098", "80001292")
         |group by unitid, adclass, hour
       """.stripMargin
    println(sqlRequest2)
    val costData = spark.sql(sqlRequest2)

    val resultDF = costData
      .join(cvrData, Seq("unitid", "adclass", "hour"), "left_outer")
      .select("unitid", "adclass", "cost", "cvr2cnt", "hour")
    resultDF
  }

  def getPactData(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcAccPact程序的结果表获取历史数据
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // read data and set redis configuration
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  cnt,
         |  total_cvr,
         |  hour
         |FROM
         |  dl_cpc.ocpc_pcvr3_history
         |WHERE $selectCondition
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getCompleteHistoryData(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcMonitor程序的结果表获取历史数据
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  cost,
         |  ctr_cnt,
         |  cvr_cnt,
         |  hour
         |FROM
         |  dl_cpc.ocpc_uid_userid_track_label2
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }


  def getCvr1HistoryData(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcMonitor程序的结果表获取历史数据
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  hour,
         |  sum(cvr1_cnt) as cvr1cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr1_data_hourly
         |WHERE
         |  media_appsid in ("80001098", "80001292")
         |  and $selectCondition
         |group by unitid,adclass, hour

       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getCvr2HistoryData(date: String, hour: String, hourCnt: Int, spark: SparkSession) :DataFrame ={
    /**
      * 按照给定的时间区间获取从OcpcMonitor程序的结果表获取历史数据
      */

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  hour,
         |  sum(cvr2_cnt) as cvr2cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr2_data_hourly
         |WHERE
         |  media_appsid in ("80001098", "80001292")
         |  and $selectCondition
         |group by unitid,adclass, hour

       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }


}