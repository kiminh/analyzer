package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2018/3/14.
  */
object InsertReportSdkVersionTrace {

  case class Info(
                   adslotId: String = "",
                   client_version: String = "",
                   data_type: String = "",
                   traceType: String = "",
                   traceOp1: String = "",
                   total: Long = 0,
                   date: String = "",
                   hour: Int = 0) {
  }

  var mariaReport2dbUrl = ""
  val mariaReport2dbProp = new Properties()

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val argDay = args(0).toString
    val argHour = args(1).toString

    val conf = ConfigFactory.load()
    mariaReport2dbUrl = conf.getString("mariadb.report2_write.url")
    mariaReport2dbProp.put("user", conf.getString("mariadb.report2_write.user"))
    mariaReport2dbProp.put("password", conf.getString("mariadb.report2_write.password"))
    mariaReport2dbProp.put("driver", conf.getString("mariadb.report2_write.driver"))

    val ctx = SparkSession
      .builder()
      .config("spark.sql.shuffle.partitions", "800")
      .appName("InsertReportSdkVersionTrace is run day is %s %s".format(argDay, argHour))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportSdkVersionTrace is run day is %s %s".format(argDay, argHour))


    val unionLogDataByAll = ctx
      .sql(
        """
          |SELECT cul.searchid,adslot_id,client_version,isfill,isshow,isclick
          |FROM dl_cpc.cpc_basedata_union_events cul
          |WHERE cul.day="%s" AND cul.hour="%s" AND cul.client_type="NATIVESDK"
          |AND cul.adslot_id in("1027423","1029077","1024335","1018971","7268884")
          |AND cul.adsrc in(0,1) AND client_version IS NOT NULL
          |AND client_version<>"" AND client_version <>"0.0.0.0"
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val searchid = x.getAs[String](0)
          val adslotId = x.getAs[String](1)
          val clientVersion = x.getAs[String](2)
          val isfill = x.getAs[Int](3).toLong
          val isshow = x.getAs[Int](4).toLong
          val isclick = x.getAs[Int](5).toLong
          val req = 1.toLong
          ((adslotId, clientVersion), (adslotId, clientVersion, "All", req, isfill, isshow, isclick))
      }
      .reduceByKey {
        (a, b) =>
          (a._1, a._2, a._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7)
      }
      .map(_._2)
      .cache()
    println("unionLogDataByAll count is", unionLogDataByAll.count())

    val reqDataByAll = unionLogDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, "request", "", x._4, argDay, argHour.toInt)
      }

    var unionLogDataByAllx = reqDataByAll

    val fillDataByAll = unionLogDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, "served_request", "", x._5, argDay, argHour.toInt)
      }

    unionLogDataByAllx = unionLogDataByAllx.union(fillDataByAll)

    val showDataByAll = unionLogDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, "impression", "", x._6, argDay, argHour.toInt)
      }
    unionLogDataByAllx = unionLogDataByAllx.union(showDataByAll)

    val clickDataByAll = unionLogDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, "click", "", x._7, argDay, argHour.toInt)
      }
    unionLogDataByAllx = unionLogDataByAllx.union(clickDataByAll)

    //获取trace信息
    val traceDataByAll = ctx
      .sql(
        """
          |SELECT DISTINCT cutl.searchid,cul.adslot_id,client_version,cutl.trace_type,cutl.trace_op1
          |FROM dl_cpc.cpc_basedata_trace_event cutl
          |INNER JOIN dl_cpc.cpc_basedata_union_events cul ON cutl.searchid=cul.searchid
          |WHERE cutl.day="%s" AND cul.day="%s"
          |AND cutl.hour="%s" AND cul.hour="%s"
          |AND cul.client_type="NATIVESDK"
          |AND cul.adslot_id in("1027423","1029077","1024335","1018971","7268884")
          |AND client_version IS NOT NULL AND client_version <>""
          |AND client_version <>"0.0.0.0"
          |AND cul.isclick>0 AND cul.adsrc=1 AND cutl.trace_type in("lpload","apkdown")
        """.stripMargin.format(argDay, argDay, argHour, argHour))
      .rdd
      .map {
        x =>
          val adslotid = x.getAs[String](1)
          val clientVersion = x.getAs[String](2)
          var traceType = x.getAs[String](3)
          var traceOp1 = x.getAs[String](4)
          var total = 1.toLong
          ((adslotid, clientVersion, traceType, traceOp1), (Info(adslotid, clientVersion, "All", traceType, traceOp1, total, argDay, argHour.toInt)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.adslotId, a.client_version, "All", a.traceType, a.traceOp1, a.total + b.total, argDay, argHour.toInt))
      }
      .map(_._2)
      .cache()
    println("traceDataByAll count is", traceDataByAll.count())

    val motivationUnionDataByAll = ctx
      .sql(
        """
          |SELECT searchid,adslot_id,client_version,isfill,isshow,isclick
          |FROM dl_cpc.cpc_basedata_union_events cul
          |WHERE cul.day="%s" AND cul.hour="%s"
          |AND cul.client_type="NATIVESDK"
          |AND cul.adslot_id in("7732880")
          |AND cul.adsrc in(0,1) AND client_version IS NOT NULL
          |AND client_version <>"" AND client_version <>"0.0.0.0"
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val searchid = x.getAs[String](0)
          val adslotId = x.getAs[String](1)
          val clientVersion = x.getAs[String](2)
          val isfill = x.getAs[Int](3).toLong
          val isshow = x.getAs[Int](4).toLong
          val isclick = x.getAs[Int](5).toLong
          val req = 1.toLong
          ((adslotId, clientVersion), (adslotId, clientVersion, "All", req, isfill, isshow, isclick))
      }
      .reduceByKey {
        (a, b) =>
          (a._1, a._2, a._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7)
      }
      .map(_._2)
      .cache()
    println("motivationUnionDataByAll count is", motivationUnionDataByAll.count())

    val reqMotivationDataByAll = motivationUnionDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, "request", "", x._4, argDay, argHour.toInt)
      }

    var unionLogMotivationDataByAllx = reqMotivationDataByAll

    val fillMotivationDataByAll = motivationUnionDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, "served_request", "", x._5, argDay, argHour.toInt)
      }

    unionLogMotivationDataByAllx = unionLogMotivationDataByAllx.union(fillMotivationDataByAll)

    val showMotivationDataByAll = motivationUnionDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, "impression", "", x._6, argDay, argHour.toInt)
      }
    unionLogMotivationDataByAllx = unionLogMotivationDataByAllx.union(showMotivationDataByAll)

    val clickMotivationDataByAll = motivationUnionDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, "click", "", x._7, argDay, argHour.toInt)
      }
    unionLogMotivationDataByAllx = unionLogMotivationDataByAllx.union(clickMotivationDataByAll)

    val motivationTraceDataByAll = ctx.sql(
      """
        |SELECT DISTINCT cutl.searchid,cul.adslot_id,client_version,cutl.trace_type,cutl.trace_op1
        |FROM dl_cpc.cpc_basedata_trace_event cutl
        |INNER JOIN (
        |   SELECT searchid,adslot_id,client_version AS client_version
        |   FROM dl_cpc.cpc_basedata_union_events cul
        |   WHERE cul.day="%s" AND cul.hour="%s" AND cul.client_type="NATIVESDK"
        |   AND cul.adslot_id in("7732880") AND cul.adsrc in(1) AND client_version IS NOT NULL
        |   AND client_version <>"" AND client_version <>"0.0.0.0" AND isclick>0
        |) cul ON cutl.searchid=cul.searchid
        |WHERE cutl.day="%s" AND cutl.hour="%s" AND cutl.trace_type in("sdk_incite")
      """.stripMargin.format(argDay, argHour, argDay, argHour))
      .rdd
      .map {
        x =>
          val adslotid = x.getAs[String](1)
          val clientVersion = x.getAs[String](2)
          var traceType = x.getAs[String](3)
          var traceOp1 = x.getAs[String](4)
          var total = 1.toLong
          ((adslotid, clientVersion, traceType, traceOp1), (Info(adslotid, clientVersion, "All", traceType, traceOp1, total, argDay, argHour.toInt)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.adslotId, a.client_version, "All", a.traceType, a.traceOp1, a.total + b.total, argDay, argHour.toInt))
      }
      .map(_._2)
      .cache()
    println("motivationTraceDataByAll count is", motivationTraceDataByAll.count())

    val allDataByAll = unionLogDataByAllx.union(traceDataByAll)

    val allMotivationDataByAll = unionLogMotivationDataByAllx.union(motivationTraceDataByAll)

    val allData = allDataByAll.union(allMotivationDataByAll)

    val allDataFrame =
      ctx.createDataFrame(allData)
        .toDF("adslot_id", "client_version", "data_type", "target_type", "trace_op1", "target_value", "date", "hour")

    allDataFrame.show(10)

    println("allDataFrame count is", allDataFrame.count())
    //
    clearReportData(argDay, argHour)

    allDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReport2dbUrl, "report2.report_sdk_version_trace", mariaReport2dbProp)
    println("InsertReportSdkVersionTrace_done")


    //---------------------------------------------------

    //---------------------------------------------------

    //---------------------------------------------------

  }

  def clearReportData(date: String, hour: String): Unit = {
    try {
      Class.forName(mariaReport2dbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariaReport2dbUrl,
        mariaReport2dbProp.getProperty("user"),
        mariaReport2dbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report2.report_sdk_version_trace where `date` = "%s" and `hour` = %d
        """.stripMargin.format(date, hour.toInt)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }

}
