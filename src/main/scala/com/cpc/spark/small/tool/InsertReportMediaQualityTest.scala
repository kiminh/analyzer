package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/11/14.
  */
object InsertReportMediaQualityTest {

  case class UnionLogInfo(
                           searchid: String = "",
                           mediaid: String = "",
                           adslotid: String = "",
                           adslot_type: Int = 0,
                           isshow: Int = 0,
                           isclick: Int = 0,
                           trace_type: String,
                           total: Int = 0,
                           planid: Int = 0) {

  }

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

    val ctx = SparkSession.builder().appName("InsertReportMediaQualityTest is run day is %s".format(argDay)).enableHiveSupport().getOrCreate()

    println("InsertReportMediaQualityTest is run day is %s".format(argDay))

    val unionLogData = ctx
      .sql(
        """
          |SELECT searchid,media_appsid,adslotid,adslot_type,isshow,isclick,planid
          |FROM dl_cpc.cpc_union_log
          |WHERE date="%s" AND ext["adclass"].int_value=132102100 AND userid=1001028 AND isshow>0 AND planid>0
        """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val mediaid = x.getString(1)
          val adslotid = x.getString(2)
          val adslot_type = x.getInt(3)
          val isshow = x.getInt(4)
          val isclick = x.getInt(5)
          val planid = x.getInt(6)
          ((adslotid, planid), (UnionLogInfo(searchid, mediaid, adslotid, adslot_type, isshow, isclick, "", 0, planid)))
      }
      .reduceByKey {
        (a, b) =>
          UnionLogInfo(a.searchid, a.mediaid, a.adslotid, a.adslot_type, a.isshow + b.isshow, a.isclick + b.isclick, "", 0, a.planid)
      }
      .repartition(50)
      .cache()
    println("unionLogData count", unionLogData.count())

    val traceData = ctx.sql(
      """
        |SELECT DISTINCT cutl.searchid,cul.media_appsid,cul.adslotid,cul.adslot_type,cutl.trace_type,cutl.duration,cul.planid
        |FROM dl_cpc.cpc_union_trace_log cutl
        |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
        |WHERE cutl.date="%s" AND cul.date="%s" AND cul.ext["adclass"].int_value=132102100 AND cul.userid=1001028 AND cul.isclick>0
      """.stripMargin.format(argDay, argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getString(0)
          val mediaid = x.getString(1)
          val adslotid = x.getString(2)
          val adslot_type = x.getInt(3)
          val duration = x.getInt(5)
          val trace_type = if (x.getString(4) == "stay") "%s%d".format(x.getString(4), x.getInt(5)) else x.getString(4)
          val planid = x.getInt(6)
          ((adslotid, planid, trace_type), (UnionLogInfo(searchid, mediaid, adslotid, adslot_type, 0, 0, trace_type, 1, planid)))
      }
      .reduceByKey {
        (a, b) =>
          UnionLogInfo(a.searchid, a.mediaid, a.adslotid, a.adslot_type, 0, 0, a.trace_type, a.total + b.total, a.planid)
      }
      .filter(_._2.trace_type.length < 200)
      .map {
        x =>
          (x._2)
      }
      .repartition(50)
      .cache()
    println("traceData count", traceData.count())

    val impressionData = unionLogData
      .map {
        x =>
          val info = x._2
          UnionLogInfo(info.searchid, info.mediaid, info.adslotid, info.adslot_type, 0, 0, "impression", info.isshow, info.planid)
      }

    val clickData = unionLogData
      .map {
        x =>
          val info = x._2
          UnionLogInfo(info.searchid, info.mediaid, info.adslotid, info.adslot_type, 0, 0, "click", info.isclick, info.planid)
      }

    val allData = impressionData
      .union(clickData)
      .union(traceData)
      .map {
        x =>
          (x.mediaid, x.adslotid, x.adslot_type, x.trace_type, x.total, argDay, x.planid)
      }
      .repartition(50)
      .cache()

    var insertDataFrame = ctx.createDataFrame(allData)
      .toDF("media_id", "adslot_id", "adslot_type", "target_type", "target_value", "date", "plan_id")
    println("insertDataFrame count", insertDataFrame.count())

    insertDataFrame.show(10)

    clearReportMediaQualityTest(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_quality_test", mariadbProp)
  }

  def clearReportMediaQualityTest(date: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_media_quality_test where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
