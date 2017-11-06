package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/10/30.
  */
object InsertAdslotHot {

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
      .appName("InsertAdslotHot run ....date %s hour %s".format(argDay, argHour))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertAdslotHot run ....date %s hour %s".format(argDay, argHour))

    val hotData = ctx
      .sql(
        """
          |SELECT media_appsid,adslotid,adslot_type,ext['touch_x'].int_value,ext['touch_y'].int_value
          |FROM dl_cpc.cpc_union_log
          |WHERE date="%s" AND hour="%s" AND (ext['touch_x'].int_value>0 OR ext['touch_y'].int_value>0)
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val media_appsid = x.getString(0)
          val adslotid = x.getString(1)
          val adslot_type = x.getInt(2)
          val touchx = x.getInt(3)
          val touchy = x.getInt(4)
          val total = 1
          val key = "%s-%d-%d".format(adslotid, touchx, touchy)
          (key, (media_appsid, adslotid, adslot_type, touchx, touchy, total))
      }
      .reduceByKey {
        (a, b) =>
          (a._1, a._2, a._3, a._4, a._5, a._6 + b._6)
      }
      .filter {
        x =>
          (x._2._1.length > 0) && (x._2._2.length > 0) && (x._2._3 > 0) && (x._2._4 >= 0) && (x._2._5 >= 0)
      }
      .map {
        x =>
          val media_appsid = x._2._1
          val adslotid = x._2._2
          val adslot_type = x._2._3
          val touchx = x._2._4
          val touchy = x._2._5
          val total = x._2._6
          var hour = argHour.toInt
          var datetime = "%s %s:00:00".format(argDay, argHour)
          var date = argDay
          (media_appsid, adslotid, adslot_type, date, hour, datetime, touchx, touchy, total)
      }
      .cache()
    println("hotData count", hotData.count())


    val hotDataFrame = ctx.createDataFrame(hotData).toDF("media_id", "adslot_id", "adslot_type", "date", "hour",
      "datetime", "touch_x", "touch_y", "total")

    clearReportHourData(argDay, argHour)

    hotDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.adslot_hot", mariadbProp)
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
          |delete from report.adslot_hot where `date` = "%s" and `hour` = %d
        """.stripMargin.format(date, hour.toInt)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
