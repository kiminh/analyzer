package com.cpc.spark.report

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

/**
  * 每5min统计每个adslotid 的pv数
  * 统计逻辑： 根据adslotid分组求sum(1)
  *
  * 输入: hive, dl_cpc.logparsed_cpc_cfg_minute
  * 输出：mysql, report2.report_hd_redirect_pv_minute
  *
  */
object InsertReport2HdRedirectPV2 {
  var report2Url = ""
  val report2Prop = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: day <date>
        """.stripMargin)
      System.exit(1)
    }
    val argDay = args(0).toString
    val argHour = args(1).toString
    val argMinute = args(2).toString
    val eminute = args(3).toString
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = ConfigFactory.load()
    report2Url = conf.getString("mariadb.report2_write.url")
    report2Prop.put("user", conf.getString("mariadb.report2_write.user"))
    report2Prop.put("password", conf.getString("mariadb.report2_write.password"))
    report2Prop.put("driver", conf.getString("mariadb.report2_write.driver"))

    val spark = SparkSession.builder()
      .appName("InsertReport2HdRedirectPVLog date " + argDay + " ,hour " + argHour + " ,minute " + argMinute)
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val sql =
      s"""
         |select
         |  aid
         |, search_timestamp
         |, log_type
         |, request_url
         |, resp_body
         |, redirect_url
         |, template_conf
         |, adslot_conf
         |, day as date
         |, hour
         |, ip
         |, ua
         |from dl_cpc.cpc_basedata_cfg_event_test
         |where day='$argDay' and hour='$argHour' and (minute between $argMinute and %s)
       """.stripMargin.format(argMinute.toInt + 4)
    println("sql: " + sql, "count: " + spark.sql(sql).count())

    val sql2 =
      s"""
         |select
         |  aid
         |, search_timestamp
         |, log_type
         |, request_url
         |, resp_body
         |, redirect_url
         |, template_conf
         |, adslot_conf
         |, date as date
         |, hour
         |, ip
         |, ua
         |from dl_cpc.cpc_basedata_cfg_event_test
         |where day='$argDay' and hour='$argHour' and (minute between %s and $eminute)
       """.stripMargin.format(argMinute.toInt + 5)
    println("sql2: " + sql2, "count: " + spark.sql(sql2).count())


    val cfgLog1 = spark.sql(sql).repartition(100)
      .filter($"log_type" === "/hdjump" || $"log_type" === "/reqhd")

    val cfgLog2 = spark.sql(sql2).repartition(100)
      .filter($"log_type" === "/hdjump" || $"log_type" === "/reqhd")


    println("cfgLog1 count: " + cfgLog1.count())
    println("cfgLog2 count: " + cfgLog2.count())
    cfgLog1.take(1).foreach(x => println(x))


    val startDate = getTimeStampByDate(argDay, argHour, argMinute) / 1000
    val middleDate = getTimeStampByDate(argDay, argHour, (argMinute.toInt + 5).toString) / 1000
    val endDate = getTimeStampByDate(argDay, argHour, (argMinute.toInt + 10).toString) / 1000

    val createTime1 = dateFormat.format(startDate * 1000)
    val createTime2 = dateFormat.format(middleDate * 1000)

    println("startDate:" + startDate + "  middleDate:" + middleDate + "  endDate:" + endDate)
    println("createTime1:" + createTime1 + "  createTime2:" + createTime2)



    //前5min cfg计算pv数写入mysql
    writeToMysql(spark, cfgLog1, argDay, createTime1)
    //后5min cfg计算pv数写入mysql
    writeToMysql(spark, cfgLog2, argDay, createTime2)

    spark.stop()

  }

  /**
    * 每5min统计每个adslotid 的pv数
    *
    * @param spark
    * @param cfgLog cfgRDD
    * @param argDay
    */
  def writeToMysql(spark: SparkSession, cfgLog: Dataset[Row], argDay: String, createTime: String): Unit = {

    var toResult = cfgLog
      .rdd
      .map { x =>(x.getAs[String]("aid"), 1)}
      .reduceByKey((x, y) => x + y)
      .map {
        case (adslotId, count) =>
          (adslotId, argDay, count, createTime)
      }

    println("count:" + toResult.count())

    val insertDataFrame = spark.createDataFrame(toResult).toDF("adslot_id", "date", "pv", "create_time")

    insertDataFrame.show(10)

    //删除当前日期和create_time的数据，防止多次写入
    clearReportHourData("report_hd_redirect_pv_minute", argDay, createTime)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(report2Url, "report2.report_hd_redirect_pv_minute", report2Prop)

    println("~~~~~~write to mysql successfully")

  }

  def clearReportHourData(tbl: String, date: String, createTime: String): Unit = {
    try {
      Class.forName(report2Prop.getProperty("driver"))
      val conn = DriverManager.getConnection(
        report2Url,
        report2Prop.getProperty("user"),
        report2Prop.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report2.%s where `date` = "%s" AND create_time="%s"
        """.stripMargin.format(tbl, date, createTime)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e)
    }
  }


  /**
    * 根据日期获取时间戳
    *
    * @param date
    * @param hour
    * @param minute
    * @return
    */
  def getTimeStampByDate(date: String, hour: String, minute: String): Long = {
    val cal = Calendar.getInstance()

    cal.set(Calendar.YEAR, date.split("-")(0).toInt)
    cal.set(Calendar.MONTH, date.split("-")(1).toInt - 1)
    cal.set(Calendar.DAY_OF_MONTH, date.split("-")(2).toInt)
    cal.set(Calendar.HOUR_OF_DAY, hour.toInt)
    cal.set(Calendar.MINUTE, minute.toInt)
    cal.set(Calendar.SECOND, 0)

    cal.getTime.getTime
  }



}