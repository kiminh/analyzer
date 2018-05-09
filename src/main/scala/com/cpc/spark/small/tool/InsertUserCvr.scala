package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.{Properties}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/10/30.
  */
object InsertUserCvr {

  var mariadbUrl = ""

  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val argDay = args(0).toString
    val argHour = args(1).toString

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession
      .builder()
      .appName("InsertUserCvr run ....date %s hour %s".format(argDay, argHour))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertUserCvr run ....date %s hour %s".format(argDay, argHour))

    val userData = ctx
      .sql(
        """
          |SELECT searchid,userid,isshow,isclick,price,media_appsid,adslotid,adslot_type
          |FROM dl_cpc.cpc_union_log
          |WHERE date="%s" AND hour="%s" AND userid>0
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val userid = x.getInt(1)
          val isshow = x.getInt(2)
          val isclick = x.getInt(3)
          val price = x.getInt(4)
          val media_appsid = x.getString(5)
          val adslotid = x.getString(6)
          val adslot_type = x.getInt(7)
          (searchid, (userid, isshow, isclick, price, media_appsid, adslotid, adslot_type, 0, 0))
      }
      .cache()
    println("userData count", userData.count())

    val jsData = ctx
      .sql(
        """
          |SELECT searchid,trace_type,duration
          |FROM dl_cpc.cpc_union_trace_log
          |WHERE date="%s" AND hour="%s"
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val trace_type = x.getString(1)
          val duration = x.getInt(2)
          var load = 0
          var active = 0
          trace_type match {
            case "load" => load += 1
            case s if s.startsWith("active") => active += 1
            //case "press" => active += 1
            case _ =>
          }
          (searchid, (load, active))
      }
      .reduceByKey {
        (a, b) =>
          (a._1 + b._1, a._2 + b._2)
      }
      .cache()
    println("jsData count", jsData.count())

    val userCvrData = userData
      .union {
        jsData
          .map {
            x =>
              (x._1, (0, 0, 0, 0, "", "", 0, x._2._1, x._2._2))
          }
      }
      .reduceByKey {
        (a, b) =>
          var userid = if (a._1 > 0) a._1 else b._1
          var isshow = if (a._2 > 0) a._2 else b._2
          var isclick = if (a._3 > 0) a._3 else b._3
          var price = if (a._4 > 0) a._4 else b._4
          var media_appsid = if (a._5.length > 0) a._5 else b._5
          var adslotid = if (a._6.length > 0) a._6 else b._6
          var adslot_type = if (a._7 > 0) a._7 else b._7
          var load = if (a._8 > 0) a._8 else b._8
          var active = if (a._9 > 0) a._9 else b._9
          (userid, isshow, isclick, price, media_appsid, adslotid, adslot_type, load, active)
      }
      .map {
        x =>
          (x._2._1.toString + x._2._6, (x._2))
      }
      .reduceByKey {
        (a, b) =>
          var userid = if (a._1 > 0) a._1 else b._1
          var isshow = a._2 + b._2
          var isclick = a._3 + b._3
          var price = 0
          if (a._3 > 0) {
            price = a._4
          }
          if (b._3 > 0) {
            price += b._4
          }
          var media_appsid = if (a._5.length > 0) a._5 else b._5
          var adslotid = if (a._6.length > 0) a._6 else b._6
          var adslot_type = if (a._7 > 0) a._7 else b._7
          var load = a._8 + b._8
          var active = a._9 + b._9
          (userid, isshow, isclick, price, media_appsid, adslotid, adslot_type, load, active)
      }
      .map {
        x =>
          var userid = x._2._1
          var isshow = x._2._2
          var isclick = x._2._3
          var price = x._2._4
          var media_appsid = 0
          try {
            media_appsid = x._2._5.toInt
          } catch {
            case e: Exception =>
          }
          var adslotid = x._2._6
          var adslot_type = x._2._7
          var load = x._2._8
          var active = x._2._9
          var hour = argHour.toInt
          var datetime = "%s %s:00:00".format(argDay, argHour)
          var date = argDay
          (userid, media_appsid, adslotid, adslot_type, date, hour, datetime, isshow, isclick, price, load, active)
      }
      .filter(_._2 > 0)
      .cache()
    println("usercvrData count", userCvrData.count())

    val userCvrDataFrame = ctx.createDataFrame(userCvrData).toDF("user_id", "media_id", "adslot_id", "adslot_type", "date", "hour",
      "datetime", "impression", "click", "cost", "load", "active")

    clearReportHourData(argDay, argHour)

    userCvrDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.user_cvr", mariadbProp)
    println("InsertUserCvr_done")
  }

  def clearReportHourData(date: String, hour: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.user_cvr where `date` = "%s" and `hour` = %d
        """.stripMargin.format(date, hour.toInt)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
