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
        |SELECT catl.trace_type,opt["game_source"]
        |FROM dl_cpc.cpc_basedata_trace_event catl
        |WHERE catl.day="%s" AND catl.hour="%s" AND catl.trace_type IS NOT NULL
      """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val traceType = x.getAs[String](0)
          val gameSource = if (!x.isNullAt(1) && x.getAs[String](1).length > 0) x.getAs[String](1) else "qtt"
          var total = 0
          if (traceType.startsWith("load_gameCenter") || traceType.startsWith("active_game")) {
            total = 1
          }
          ((traceType, gameSource), (traceType, gameSource, total))
      }
      .filter(_._2._3 > 0)
      .reduceByKey {
        (a, b) =>
          (a._1, a._2, a._3 + b._3)
      }
      .map {
        x =>
          val traceType = x._2._1
          val gameSource = x._2._2
          val total = x._2._3.toLong
          val date = argDay
          val hour = argHour.toInt
          (traceType, total, gameSource, date, hour)
      }
      .repartition(50)
      .cache()
    println("traceData count", traceData.count())

    val uvSeq = ctx.sql(
      """
        |SELECT DISTINCT catl.trace_type,opt["device"],opt["game_source"]
        |FROM dl_cpc.cpc_basedata_trace_event catl
        |WHERE catl.day="%s" AND catl.hour="%s" AND catl.trace_type IS NOT NULL
        |AND catl.trace_op1 IS NOT NULL AND catl.trace_op2 IS NOT NULL
        |AND catl.ip IS NOT NULL AND catl.trace_type="load_gameCenter"
      """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          //val device = if (!x.isNullAt(2)) x.getAs[String](1) else ""
          val gameSource = if (!x.isNullAt(2) && x.getAs[String](2).length > 0) x.getAs[String](2) else "qtt"
          ((gameSource), (1.toLong))
      }
      .reduceByKey {
        (a, b) =>
          (a + b)
      }
      .map {
        x =>
          val gameSource = x._1
          val pvCount = x._2
          ("gameCenter_uv", pvCount, gameSource, argDay, argHour.toInt)
      }
      .take(100)
      .toSeq

    println("uvSeq is ", uvSeq.length)

    val uvData = ctx.sparkContext.parallelize(uvSeq)

    var insertDataFrame = ctx.createDataFrame(uvData.union(traceData))
      .toDF("target_type", "target_value", "game_source", "date", "hour")

    println("insertDataFrame count", insertDataFrame.count())

    insertDataFrame.show(5)

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
