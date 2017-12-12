package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/10/30.
  */
object InsertReportInteractAdslotUser {

  var mariadbUrl = ""

  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val argDay = args(0).toString

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession
      .builder()
      .appName("InsertReportInteractAdslotUser run ....date %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportInteractAdslotUser run ....date %s".format(argDay))

    val allData = ctx
      .sql(
        """
          |SELECT userid,media_appsid,adslotid,isshow,isclick,hour,price,isfill
          |FROM dl_cpc.cpc_union_log
          |WHERE date="%s" AND adslot_type=3
        """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val userid = x.getInt(0)
          val media_appsid = x.getString(1)
          val adslotid = x.getString(2)
          val isshow = x.getInt(3)
          val isclick = x.getInt(4)
          val hour = x.getString(5)
          val request = 1
          val price = if (isclick > 0) x.getInt(6) else 0
          val isfill = x.getInt(7)

          val key = "%d-%s-%s-%s".format(userid, media_appsid, adslotid, hour)
          (key, (userid, media_appsid, adslotid, isshow, isclick, hour, request, price, isfill))
      }
      .reduceByKey {
        (a, b) =>
          (a._1, a._2, a._3,a._4 + b._4, a._5 + b._5, a._6, a._7 + b._7, a._8 + b._8, a._9 + b._9)
      }
      .filter {
        x =>
          (x._2._2.length > 0) && (x._2._3.length > 0)
      }
      .map {
        x =>
          val userid = x._2._1
          val media_appsid = x._2._2
          val adslotid = x._2._3
          val isshow = x._2._4
          val isclick = x._2._5
          val hour = x._2._6
          val request = x._2._7
          val datetime = "%s %s:00:00".format(argDay, hour)
          val date = argDay
          val price = x._2._8
          val isfill = x._2._9
          (userid, media_appsid, adslotid, date, hour.toInt, datetime, request, isshow, isclick, price, isfill)
      }
      .cache()
    println("allData count", allData.count())


    val hotDataFrame = ctx.createDataFrame(allData).toDF("user_id", "media_id", "adslot_id", "date", "hour",
      "datetime", "request", "impression", "click", "cost", "served_request")

    clearReportSupportUserDataByDay(argDay)

    hotDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_interact_adslot_user", mariadbProp)
  }

  def clearReportSupportUserDataByDay(date: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_interact_adslot_user where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
