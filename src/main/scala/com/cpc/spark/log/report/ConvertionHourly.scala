package com.cpc.spark.log.report

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  *  count Conversion number.
  *  data source: dl_cpc.ml_cvr_feature_v1、dl_cpc.ml_cvr_feature_v2
  *  output table(mysql): report.convertion_hourly
  */
object ConvertionHourly {
  var mariadbUrl = ""
  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: ConvertionHourly <date> <hour>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0)
    val hour = args(1)
    println("*******************")
    println("date:" + date)
    println("hour:" + hour)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val spark = SparkSession.builder()
      .appName("cpc get trace hour report from %s/%s".format(date, hour))
      .enableHiveSupport()
      .getOrCreate()

    val sql=
      s"""
         |select user
       """.stripMargin

  }

  def clearReportHourData(tbl: String, date: String, hour: String): Unit = {
    try {
      println("~~~~~clear ConvertionHourly Data~~~~~")
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.%s where `date` = "%s" and `hour` = %d
        """.stripMargin.format(tbl, date, hour.toInt)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
