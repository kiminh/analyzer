package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/11/14.
  */
object InsertReportGameCenter {

  case class UnionLogInfo(
                           searchid: String = "",
                           mediaid: String = "",
                           adslotid: String = "",
                           adslot_type: Int = 0,
                           isshow: Int = 0,
                           isclick: Int = 0,
                           trace_type: String,
                           total: Int) {

  }

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
      .appName("InsertReportGameCenter is run day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportGameCenter is run day is %s %s".format(argDay, argHour))


    val traceData = ctx.sql(
      """
        |SELECT catl.trace_type
        |FROM dl_cpc.cpc_all_trace_log catl
        |WHERE catl.date="%s" AND catl.hour="%s" AND catl.trace_type IS NOT NULL
      """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val traceType = x.getString(0)
          var total = 0
          if (traceType.startsWith("load_gameCenter") || traceType.startsWith("active_game")) {
            total = 1
          }
          (traceType, (traceType, total))
      }
      .filter(_._2._2 > 0)
      .reduceByKey {
        (a, b) =>
          (a._1, a._2 + b._2)
      }
      .map {
        x =>
          val traceType = x._2._1
          val total = x._2._2.toLong
          val date = argDay
          val hour = argHour.toInt
          (traceType, total, date, hour)
      }
      .repartition(50)
      .cache()
    println("traceData count", traceData.count())

    val pvCount = ctx.sql(
      """
        |SELECT catl.trace_type,ip
        |FROM dl_cpc.cpc_all_trace_log catl
        |WHERE catl.date="%s" AND catl.hour="%s" AND catl.trace_type IS NOT NULL
        |AND catl.trace_op1 IS NOT NULL AND catl.trace_op2 IS NOT NULL
        |AND catl.ip IS NOT NULL
      """.stripMargin.format(argDay, argHour))
      .rdd
      .filter(_.getString(0) == "load_gameCenter")
      .map {
        x =>
          val ip = x.getString(1)
          (ip, (1))
      }
      .reduceByKey {
        (a, b) =>
          (1)
      }
      .count()
    println("pvCount is ", pvCount)

    val pvData = ctx.sparkContext.parallelize(Seq(("gameCenter_uv", pvCount, argDay, argHour.toInt)))


    var insertDataFrame = ctx.createDataFrame(pvData.union(traceData))
      .toDF("target_type", "target_value", "date", "hour")

    println("insertDataFrame count", insertDataFrame.count())


    insertDataFrame.show(30)

    clearReportGameCenter(argDay, argHour)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_game_center", mariadbProp)
    println("InsertReportGameCenter_done")
  }

  def clearReportGameCenter(date: String, hour: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_game_center where `date` = "%s" AND `hour` = %d
        """.stripMargin.format(date, hour.toInt)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
