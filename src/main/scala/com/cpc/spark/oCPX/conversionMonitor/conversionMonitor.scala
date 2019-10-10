package com.cpc.spark.oCPX.conversionMonitor

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


/**
  * @author 作者 :wangjun
  * @version 创建时间：2019-05-23
  * @desc
  */

object conversionMonitor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("[ocpc-monitor] extract log data")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val dayCnt = args(1).toInt
    val minCV = args(2).toInt
    println("parameters:")
    println(s"date=$date")

    // todo 抽取点击数据和消费数据，按照unitid, userid, conversion_goal, conversion_from, is_ocpc做grouby
    val clickData = getClickCost(date, dayCnt, spark)
    // todo 抽取转化数据，按照unitid, userid, conversion_goal, conversion_from做groupby
    val cvData = getCVdata(date, dayCnt, spark)
    // todo 按照最小点击数限额，找到对应conversion_goal与conversion_from中无转化的单元
    // todo 根据是否是ocpc单元选择发送报警邮件


  }

  def getCVdata(date: String, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -dayCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val date1 = tmpDate(0)
    val selectCondition = s"`date` between '$date1' and '$date'"

    // 收集转化数据
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  conversion_from
         |FROM
         |  dl_cpc.ocpc_cvr_log_hourly
         |WHERE
         |  $selectCondition
         |""".stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)
    val resultDF = rawData
      .groupBy("unitid", "userid", "conversion_goal", "conversion_from")
      .agg(
        countDistinct(col("searchid")).alias("cv")
      )
      .select("unitid", "userid", "conversion_goal", "conversion_from", "cv")

    resultDF
  }

  def getClickCost(date: String, dayCnt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -dayCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val date1 = tmpDate(0)
    val selectCondition = s"`date` between '$date1' and '$date'"

    // 收集点击和消费数据
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  conversion_from,
         |  is_ocpc,
         |  isclick,
         |  price * 0.01 as price
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND isclick = 1
         |""".stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)
    val resultDF = rawData
      .groupBy("unitid", "userid", "conversion_goal", "conversion_from", "is_ocpc")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("price")).alias("price")
      )
      .select("unitid", "userid", "conversion_goal", "conversion_from", "is_ocpc", "click", "price")

    resultDF
  }


}
