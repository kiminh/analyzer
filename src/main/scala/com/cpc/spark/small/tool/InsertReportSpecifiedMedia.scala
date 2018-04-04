package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/11/14.
  */
object InsertReportSpecifiedMedia {

  case class UnionLogInfo(
                           searchid: String = "",
                           userid: Int = 0,
                           unitid: Int = 0,
                           ideaid: Int = 0,
                           isshow: Int = 0,
                           isclick: Int = 0,
                           trace_type: String,
                           total: Int,
                           hour: Int = 0) {

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

    val ctx = SparkSession.builder().appName("InsertReportSpecifiedMedia is run day is %s %s".format(argDay, argHour)).enableHiveSupport().getOrCreate()

    println("InsertReportSpecifiedMedia is run day is %s %s".format(argDay, argHour))

    val unionLogData = ctx
      .sql(
        """
          |SELECT searchid,userid,unitid,ideaid,isshow,isclick,hour
          |FROM dl_cpc.cpc_union_log cul
          |WHERE cul.date="%s" AND cul.hour="%s" AND cul.media_appsid="80000001" AND cul.adslot_type=1 AND cul.isshow>0
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val userid = x.getInt(1)
          val unitid = x.getInt(2)
          val ideaid = x.getInt(3)
          val isshow = x.getInt(4)
          val isclick = x.getInt(5)
          val hour = x.getString(6).toInt
          ((ideaid), UnionLogInfo(searchid, userid, unitid, ideaid, isshow, isclick, "", 0, hour))

      }
      .reduceByKey {
        (a, b) =>
          UnionLogInfo(a.searchid, a.userid, a.unitid, a.ideaid, a.isshow + b.isshow, a.isclick + b.isclick, "", 0, a.hour)
      }
      .repartition(50)
      .cache()
    println("unionLogData count", unionLogData.count())

    val traceData = ctx.sql(
      """
        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.duration,cutl.hour,
        |cul.userid, cul.unitid,cul.ideaid
        |FROM dl_cpc.cpc_union_trace_log cutl
        |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
        |WHERE cutl.date="%s" AND cul.date="%s"  AND cutl.hour="%s" AND cul.hour="%s"
        |AND cul.media_appsid="80000001" AND cul.adslot_type=1 AND cul.isclick>0
      """.stripMargin.format(argDay, argDay, argHour, argHour))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val duration = x.getInt(2)
          val trace_type = if (x.getString(1) == "stay") "%s%d".format(x.getString(1), x.getInt(2)) else x.getString(1)
          val hour = x.getString(3).toInt
          val userid = x.getInt(4)
          val unitid = x.getInt(5)
          val ideaid = x.getInt(6)
          ((ideaid, trace_type), UnionLogInfo(searchid, userid, unitid, ideaid, 0, 0, trace_type, 1, hour))
      }
      .reduceByKey {
        (a, b) =>
          UnionLogInfo(a.searchid, a.userid, a.unitid, a.ideaid, 0, 0, a.trace_type, a.total + b.total, a.hour)
      }
      .map {
        x =>
          (x._2)
      }
      .filter(_.userid > 0)
      .repartition(50)
      .cache()
    println("traceData count", traceData.count())

    val impressionData = unionLogData
      .map {
        x =>
          val info = x._2
          UnionLogInfo(info.searchid, info.userid, info.unitid, info.ideaid, 0, 0, "impression", info.isshow, info.hour)
      }

    val clickData = unionLogData
      .filter(_._2.isclick > 0)
      .map {
        x =>
          val info = x._2
          UnionLogInfo(info.searchid, info.userid, info.unitid, info.ideaid, 0, 0, "click", info.isclick, info.hour)
      }

    val allData = impressionData
      .union(clickData)
      .union(traceData)
      .map {
      x =>
        (x.userid, x.unitid, x.ideaid, x.trace_type, x.total, argDay, x.hour)
    }
      .repartition(50)
      .cache()

    var insertDataFrame = ctx.createDataFrame(allData)
      .toDF("user_id", "unit_id", "idea_id", "target_type", "target_value", "date", "hour")

    println("insertDataFrame count", insertDataFrame.count())

    insertDataFrame.show(10)

    clearReportSpecifiedMedia(argDay, argHour)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_specified_media", mariadbProp)
    println("InsertReportSpecifiedMedia_done")
  }

  def clearReportSpecifiedMedia(date: String, hour: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_specified_media where `date` = "%s" AND `hour`=%d
        """.stripMargin.format(date, hour.toInt)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
