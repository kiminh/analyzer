package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/11/14.
  */
object InsertReportGameCenterProduct {

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
      .appName("InsertReportGameCenterProduct is run day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportGameCenterProduct is run day is %s %s".format(argDay, argHour))


    val traceData = ctx.sql(
      """
        |SELECT catl.trace_type,catl.trace_op1,catl.trace_op2,catl.trace_op3,opt["game_source"]
        |FROM dl_cpc.logparsed_cpc_trace_minute catl
        |WHERE catl.thedate="%s" AND catl.thehour="%s" AND catl.trace_type IS NOT NULL
        |AND catl.trace_op1 IS NOT NULL AND catl.trace_op2 IS NOT NULL
      """.stripMargin.format(argDay, argHour))
      .rdd
      .filter(_.getString(0).startsWith("active_game"))
      .map {
        x =>
          val traceType = x.getString(0)
          val traceOp1 = x.getString(1)
          val traceOp2 = x.getString(2)
          val traceOp3 = x.getString(3)
          val gameSource = if (x.get(4) != null && x.getString(4).length > 0) x.getString(4) else "qtt"

          ((traceOp2, traceOp3, gameSource), (1.toLong))
      }
      .reduceByKey {
        (a, b) =>
          (a + b)
      }
      .map {
        x =>
          var traceOp2 = 0
          try {
            traceOp2 = x._1._1.toInt
          } catch {
            case e: Exception =>
          }

          var traceOp3 = x._1._2

          val gameSource = x._1._3

          val total = x._2
          val date = argDay
          val hour = argHour.toInt
          (traceOp3, traceOp2, gameSource, total, date, hour)
      }
      .filter {
        x =>
          (x._2 > 0 && x._1.length > 0)
      }
      .cache()


    var insertDataFrame = ctx.createDataFrame(traceData)
      .toDF("product_type", "product_id", "game_source", "total", "date", "hour")

    println("insertDataFrame count", insertDataFrame.count())

    insertDataFrame.show(5)

    clearReportGameCenterProduct(argDay, argHour)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_game_center_product", mariadbProp)

    println("InsertReportGameCenterProduct_done")
  }

  def clearReportGameCenterProduct(date: String, hour: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_game_center_product where `date` = "%s" AND `hour` = %d
        """.stripMargin.format(date, hour.toInt)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
