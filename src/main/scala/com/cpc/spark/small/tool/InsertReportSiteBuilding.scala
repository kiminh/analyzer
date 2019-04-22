package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/11/14.
  */
object InsertReportSiteBuilding {

  case class UnionLogInfo(
                           searchid: String = "",
                           userid: Int = 0,
                           unitid: Int = 0,
                           ideaid: Int = 0,
                           isshow: Int = 0,
                           isclick: Int = 0,
                           trace_type: String,
                           total: Int,
                           siteid: Int = 0,
                           price: Int = 0,
                           traceOp1: String = "") {

  }

  var mariaAdvdbUrl = ""
  val mariaAdvdbProp = new Properties()

  var mariaReportdbUrl = ""
  val mariaReportdbProp = new Properties()

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val argDay = args(0).toString


    val confDav = ConfigFactory.load()
    mariaAdvdbUrl = confDav.getString("mariadb.adv.url")
    mariaAdvdbProp.put("user", confDav.getString("mariadb.adv.user"))
    mariaAdvdbProp.put("password", confDav.getString("mariadb.adv.password"))
    mariaAdvdbProp.put("driver", confDav.getString("mariadb.adv.driver"))

    val conf = ConfigFactory.load()
    mariaReportdbUrl = conf.getString("mariadb.url")
    mariaReportdbProp.put("user", conf.getString("mariadb.user"))
    mariaReportdbProp.put("password", conf.getString("mariadb.password"))
    mariaReportdbProp.put("driver", conf.getString("mariadb.driver"))


    val ctx = SparkSession.builder().appName("InsertReportSiteBuilding is run day is %s".format(argDay)).enableHiveSupport().getOrCreate()

    println("InsertReportSiteBuilding is run day is %s".format(argDay))

    val unionLogData = ctx
      .sql(
        """
          |SELECT searchid,userid,unitid,ideaid,isshow,isclick,price,siteid
          |FROM dl_cpc.cpc_basedata_union_events cul
          |WHERE cul.day="%s" AND cul.isshow>0 AND cul.siteid>0
        """.stripMargin.format(argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getAs[String](0)
          val userid = x.getAs[Int](1)
          val unitid = x.getAs[Int](2)
          val ideaid = x.getAs[Int](3)
          val isshow = if (x.getAs[Int](5) > 0) 1 else x.getAs[Int](4)
          val isclick = x.getAs[Int](5)
          val price = if (isclick > 0) x.getAs[Int](6) else 0
          val siteid = x.get(7).toString.toInt
          ((siteid), UnionLogInfo(searchid, userid, unitid, ideaid, isshow, isclick, "", 0, siteid, price))

      }
      .reduceByKey {
        (a, b) =>
          UnionLogInfo(a.searchid, a.userid, a.unitid, a.ideaid, a.isshow + b.isshow, a.isclick + b.isclick, "", 0, a.siteid, a.price + b.price)
      }
      .filter(_._2.siteid > 0)
      .repartition(50)
      .cache()
    println("unionLogData count", unionLogData.count())

    val traceData = ctx.sql(
      """
        |SELECT DISTINCT cutl.searchid,cutl.trace_type,cutl.duration,
        |cul.userid, cul.unitid,cul.ideaid,cutl.trace_op1,siteid
        |FROM dl_cpc.cpc_basedata_trace_event cutl
        |INNER JOIN dl_cpc.cpc_basedata_union_events cul ON cutl.searchid=cul.searchid
        |WHERE cutl.day="%s" AND cul.day="%s" AND cul.isclick>0 AND cul.ideaid>0 AND cul.userid>0 AND cul.siteid>0
      """.stripMargin.format(argDay, argDay))
      .rdd
      .map {
        x =>
          val searchid = x.getAs[String](0)
          val duration = x.getAs[Int](2)
          val trace_type = if (x.getAs[String](1) == "stay") "%s%d".format(x.getAs[String](1), x.getAs[Int](2)) else x.getAs[String](1)
          val userid = x.getAs[Int](3)
          val unitid = x.getAs[Int](4)
          val ideaid = x.getAs[Int](5)
          var traceOp1 = ""
          if ((trace_type == "apkdown") || (trace_type == "lpload")) {
            traceOp1 = x.getAs[String](6)
          }
          val siteid = x.getAs[Int](7)
          ((siteid, trace_type), UnionLogInfo(searchid, userid, unitid, ideaid, 0, 0, trace_type, 1, siteid, 0, traceOp1))
      }
      .map {
        x =>
          ((x._2.siteid, x._2.trace_type, x._2.traceOp1),
            UnionLogInfo(x._2.searchid, x._2.userid, x._2.unitid, x._2.ideaid, 0, 0, x._2.trace_type, 1, x._2.siteid, 0, x._2.traceOp1))
      }
      .reduceByKey {
        (a, b) =>
          UnionLogInfo(a.searchid, a.userid, a.unitid, a.ideaid, 0, 0, a.trace_type, a.total + b.total, a.siteid, 0, a.traceOp1)
      }
      .map {
        x =>
          (x._2)
      }
      .filter(x=>(x.siteid > 0 && x.trace_type.length<200 && x.traceOp1.length<200))
      .repartition(50)
      .cache()
    println("traceData count", traceData.count())

    val impressionData = unionLogData
      .map {
        x =>
          val info = x._2
          UnionLogInfo(info.searchid, info.userid, info.unitid, info.ideaid, 0, 0, "impression", info.isshow, info.siteid)
      }

    val clickData = unionLogData
      .filter(_._2.isclick > 0)
      .map {
        x =>
          val info = x._2
          UnionLogInfo(info.searchid, info.userid, info.unitid, info.ideaid, 0, 0, "click", info.isclick, info.siteid)
      }

    val priceData = unionLogData
      .filter(_._2.isclick > 0)
      .map {
        x =>
          val info = x._2
          UnionLogInfo(info.searchid, info.userid, info.unitid, info.ideaid, 0, 0, "price", info.price, info.siteid)
      }

    val allData = impressionData
      .union(clickData)
      .union(traceData)
      .union(priceData)
      .map {
        x =>
          (x.siteid, x.userid, x.trace_type, x.total, argDay, x.traceOp1)
      }
      .repartition(50)
      .cache()

    var insertDataFrame = ctx.createDataFrame(allData)
      .toDF("site_id", "user_id", "target_type", "target_value", "date", "trace_op1")

    println("insertDataFrame count", insertDataFrame.count())

    insertDataFrame.show(20)


    clearReportSiteBuilding(argDay)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReportdbUrl, "report.report_site_building", mariaReportdbProp)
  }

  def clearReportSiteBuilding(date: String): Unit = {
    try {
      Class.forName(mariaReportdbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariaReportdbUrl,
        mariaReportdbProp.getProperty("user"),
        mariaReportdbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_site_building where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
