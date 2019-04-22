package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.util.Success

/**
  * Created by wanli on 2018/5/8.
  */
object InsertReportInteractTrace {

  var mariaReportdbUrl = ""
  val mariaReportdbProp = new Properties()

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val argDay = args(0).toString

    val conf = ConfigFactory.load()
    mariaReportdbUrl = conf.getString("mariadb.url")
    mariaReportdbProp.put("user", conf.getString("mariadb.user"))
    mariaReportdbProp.put("password", conf.getString("mariadb.password"))
    mariaReportdbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession
      .builder()
      .config("spark.debug.maxToStringFields", "2000")
      .appName("InsertReportDspIdea is run day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportInteractTrace is run day is %s".format(argDay))

    //hd_
    val allData = ctx.sql(
      """
        |SELECT trace_op1,count(*)
        |FROM dl_cpc.cpc_basedata_trace_event
        |WHERE day="%s" AND trace_type LIKE "%s"
        |GROUP BY trace_op1
      """.stripMargin.format(argDay, "hd_load_%"))
      .rdd
      .map {
        x =>
          var total = x.get(1).toString.toLong
          var traceOp1 = 0
          val tmpTraceOp1 = x.get(0).toString
          if (tmpTraceOp1.length > 0) {
            var tmpOsV = scala.util.Try(tmpTraceOp1.toInt)
            traceOp1 = tmpOsV match {
              case Success(_) => tmpTraceOp1.toInt;
              case _ => 0
            }
          }
          (traceOp1, "load", total, argDay)
      }
      .filter(_._1 > 0)
      .cache()

    println("allData count is", allData.count())

    val insertDataFrame = ctx.createDataFrame(allData).toDF("adslot_id", "target_type", "target_value", "date")

    insertDataFrame.show(20)

    clearReportInteractTrace(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReportdbUrl, "report.report_interact_trace", mariaReportdbProp)

  }

  def clearReportInteractTrace(date: String): Unit = {
    try {
      Class.forName(mariaReportdbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariaReportdbUrl,
        mariaReportdbProp.getProperty("user"),
        mariaReportdbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_interact_trace where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
