package com.cpc.spark.log.report

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Roy on 2017/4/26.
  * new owner: fym (190428).
  */
object GetDayUv {

  var mariadbUrl = ""

  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: GetDayUv <hive_table> <day_before>
           |
        """.stripMargin)
      System.exit(1)
    }
    val table = args(0)
    val dayBefore = args(1).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password",conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession.builder()
      .appName("[cpc-anal] report_media_uv_daily %s".format(date))
      .enableHiveSupport()
      .getOrCreate()

    val dataToGo = ctx.sql(
      s"""
         |select
         |  media_appsid as media_id
         |  , adslot_id
         |  , adslot_type
         |  , day as `date`
         |  , count(distinct uid) as uniq_user
         |from dl_cpc.cpc_basedata_union_events
         |where `day` = "%s"
         |and adslot_id > 0
         |and isshow = 1
         |group by
         |  media_appsid
         |  , adslot_id
         |  , adslot_type
         |  , day
       """.stripMargin.format(date))

    clearReportHourData(date)
    dataToGo
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_uv_daily", mariadbProp)

    println("done", dataToGo.count())
    ctx.stop()
  }

  def clearReportHourData(date: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_media_uv_daily where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e)
    }
  }
}

