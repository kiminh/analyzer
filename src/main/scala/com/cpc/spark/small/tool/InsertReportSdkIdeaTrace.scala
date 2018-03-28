package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2018/3/14.
  */
object InsertReportSdkIdeaTrace {

  case class Info(
                   userid: Int = 0,
                   planid: Int = 0,
                   unitid: Int = 0,
                   ideaid: Int = 0,
                   isshow: Long = 0,
                   isclick: Long = 0,
                   traceType: String = "",
                   traceOp1: String = "",
                   total: Long = 0,
                   date: String = "",
                   hour: Int = 0,
                   data_type: String = "") {

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
      .config("spark.sql.shuffle.partitions", "800")
      .appName("InsertReportSdkDownTrace is run day is %s %s".format(argDay, argHour))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportSdkDownTrace is run day is %s %s".format(argDay, argHour))

    /**
      * 获取sdk下载流量信息
      */

    //获取sdk下载流量信息
    val unionLogDataByDownSdk = ctx
      .sql(
        """
          |SELECT userid,planid,unitid,ideaid,isshow,isclick
          |FROM dl_cpc.cpc_union_log cul
          |WHERE cul.date="%s"
          |AND cul.ext["client_type"].string_value="NATIVESDK"
          |AND cul.interaction=2
        """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val userid = x.getInt(0)
          val planid = x.getInt(1)
          val unitid = x.getInt(2)
          val ideaid = x.getInt(3)
          val isshow = x.get(4).toString.toLong
          val isclick = x.get(5).toString.toLong
          ((ideaid), (Info(userid, planid, unitid, ideaid, isshow, isclick)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.userid, a.planid, a.unitid, a.ideaid, a.isshow + b.isshow, a.isclick + b.isclick))
      }
      .map(_._2)
      .cache()
    println("unionLogDataByDownSdk count is", unionLogDataByDownSdk.count())

    //获取sdk下载流量信息
    val jsTraceDataByDownSdk = ctx
      .sql(
        """
          |SELECT DISTINCT cutl.searchid,
          |cul.userid,cul.planid,cul.unitid,cul.ideaid,
          |cutl.trace_type,cutl.trace_op1
          |FROM dl_cpc.cpc_union_trace_log cutl
          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
          |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.trace_type in("apkdown")
          |AND cul.ext["client_type"].string_value="NATIVESDK"
          |AND cul.interaction=2
          |AND cul.isclick>0
        """.stripMargin.format(argDay, argDay))
      .rdd
      .map {
        x =>
          val userid = x.getInt(1)
          val planid = x.getInt(2)
          val unitid = x.getInt(3)
          val ideaid = x.getInt(4)
          var traceType = x.getString(5)
          var traceOp1 = x.getString(6)
          var total = 1.toLong
          ((ideaid, traceType, traceOp1), (Info(userid, planid, unitid, ideaid, 0, 0, traceType, traceOp1, total)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.userid, a.planid, a.unitid, a.ideaid, 0, 0, a.traceType, a.traceOp1, a.total + b.total))
      }
      .map(_._2)
      .cache()
    println("jsTraceDataByDownSdk count is", jsTraceDataByDownSdk.count())


    /**
      * 下载sdk流量
      */
    var downSdkShow = unionLogDataByDownSdk
      .map {
        x =>
          (x.userid, x.planid, x.unitid, x.ideaid, "impression", x.traceOp1, x.isshow, argDay, "DownSdk")
      }

    var downSdkClick = unionLogDataByDownSdk
      .map {
        x =>
          (x.userid, x.planid, x.unitid, x.ideaid, "click", x.traceOp1, x.isclick, argDay, "DownSdk")
      }

    var downSdkApkdown = jsTraceDataByDownSdk
      .map {
        x =>
          (x.userid, x.planid, x.unitid, x.ideaid, "apkdown", x.traceOp1, x.total, argDay, "DownSdk")
      }


    var allData = downSdkShow
      .union(downSdkClick)
      .union(downSdkApkdown)
    //      .take(10000)
    //      .foreach(println)

    val allDataFrame =
      ctx.createDataFrame(allData)
        .toDF("user_id", "plan_id", "unit_id", "idea_id", "target_type", "trace_op1", "target_value", "date", "data_type")

    allDataFrame.show(10)

    clearReportSdkIdeaTrace(argDay)

    allDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_sdk_idea_trace", mariadbProp)
    //---------------------------------------------------

    //---------------------------------------------------

    //---------------------------------------------------

  }

  def clearReportSdkIdeaTrace(date: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_sdk_idea_trace where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
