package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2018/3/14.
  */
object InsertReportSdkTrace {

  case class Info(
                   mediaId: String = "",
                   adslotId: String = "",
                   adslotType: Int = 0,
                   req: Long = 0,
                   isfill: Long = 0,
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
      .appName("InsertReportSdkTrace is run day is %s %s".format(argDay, argHour))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportSdkTrace is run day is %s %s".format(argDay, argHour))

    /**
      * 获取全量非sdk流量信息
      */

    //获取全量非sdk流量信息
    val unionLogDataByAllNoSdk = ctx
      .sql(
        """
          |SELECT media_appsid,adslotid,adslot_type,isfill,isshow,isclick
          |FROM dl_cpc.cpc_union_log cul
          |WHERE cul.date="%s" AND cul.hour="%s" AND cul.ext["client_type"].string_value<>"NATIVESDK"
          |AND cul.media_appsid in ("80000001","80000002", "80000006", "800000062", "80000064", "80000066", "80000141")
          |AND cul.adslotid in("1024335","1024336","1024902","1025156","1025164","1026276","1026437","1026459","1026890",
          |"1026966","1026992","1027423","1028214","1028626","7074294","7647654","1029077") AND cul.adsrc=1
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val mediaId = x.getString(0)
          val adslotId = x.getString(1)
          val adslotType = x.getInt(2)
          val isfill = x.get(3).toString.toLong
          val isshow = x.get(4).toString.toLong
          val isclick = x.get(5).toString.toLong
          val req = 1.toLong
          ((adslotId), (Info(mediaId, adslotId, adslotType, req, isfill, isshow, isclick)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.mediaId, a.adslotId, a.adslotType, a.req + b.req, a.isfill + b.isfill, a.isshow + b.isshow, a.isclick + b.isclick))
      }
      .map(_._2)
      .cache()
    //println("unionLogDataByAllNoSdk count is", unionLogDataByAllNoSdk.count())

    /**
      * 获取全量sdk流量信息
      */
    //获取全量sdk流量信息
    val unionLogDataByAllSdk = ctx
      .sql(
        """
          |SELECT media_appsid,adslotid,adslot_type,isfill,isshow,isclick
          |FROM dl_cpc.cpc_union_log cul
          |WHERE cul.date="%s" AND cul.hour="%s"
          |AND cul.ext["client_type"].string_value="NATIVESDK" AND cul.adsrc=1
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val mediaId = x.getString(0)
          val adslotId = x.getString(1)
          val adslotType = x.getInt(2)
          val isfill = x.get(3).toString.toLong
          val isshow = x.get(4).toString.toLong
          val isclick = x.get(5).toString.toLong
          val req = 1.toLong
          ((adslotId), (Info(mediaId, adslotId, adslotType, req, isfill, isshow, isclick)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.mediaId, a.adslotId, a.adslotType, a.req + b.req, a.isfill + b.isfill, a.isshow + b.isshow, a.isclick + b.isclick))
      }
      .map(_._2)
      .cache()
    //println("unionLogDataByAllSdk count is", unionLogDataByAllSdk.count())


    /**
      * 获取网赚非sdk流量信息
      */

    //获取网赚非sdk流量信息
    val unionLogDataByWzNoSdk = ctx
      .sql(
        """
          |SELECT media_appsid,adslotid,adslot_type,isfill,isshow,isclick
          |FROM dl_cpc.cpc_union_log cul
          |WHERE cul.date="%s" AND cul.hour="%s" AND cul.ext["client_type"].string_value<>"NATIVESDK"
          |AND cul.media_appsid in ("80000001","80000002", "80000006", "800000062", "80000064", "80000066", "80000141")
          |AND cul.ext["adclass"].int_value=110110100
          |AND cul.adslotid in("1024335","1024336","1024902","1025156","1025164","1026276","1026437","1026459","1026890",
          |"1026966","1026992","1027423","1028214","1028626","7074294","7647654","1029077") AND cul.adsrc=1
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val mediaId = x.getString(0)
          val adslotId = x.getString(1)
          val adslotType = x.getInt(2)
          val isfill = x.get(3).toString.toLong
          val isshow = x.get(4).toString.toLong
          val isclick = x.get(5).toString.toLong
          val req = 1.toLong
          ((adslotId), (Info(mediaId, adslotId, adslotType, req, isfill, isshow, isclick)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.mediaId, a.adslotId, a.adslotType, a.req + b.req, a.isfill + b.isfill, a.isshow + b.isshow, a.isclick + b.isclick))
      }
      .map(_._2)
      .cache()
    //println("unionLogDataByWzNoSdk count is", unionLogDataByWzNoSdk.count())

    //获取网赚非sdk流量信息
    val jsTraceDataByWzNoSdk = ctx
      .sql(
        """
          |SELECT DISTINCT cutl.searchid,cul.media_appsid,cul.adslotid,cul.adslot_type,
          |cutl.trace_type,cutl.trace_op1
          |FROM dl_cpc.cpc_union_trace_log cutl
          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
          |WHERE cutl.date="%s" AND cul.date="%s"
          |AND cutl.hour="%s" AND cul.hour="%s" AND cutl.trace_type in("load","active5","disactive")
          |AND cul.ext["client_type"].string_value<>"NATIVESDK"
          |AND cul.media_appsid in ("80000001","80000002", "80000006", "800000062", "80000064", "80000066", "80000141")
          |AND cul.ext["adclass"].int_value=110110100
          |AND cul.adslotid in("1024335","1024336","1024902","1025156","1025164","1026276","1026437","1026459","1026890",
          |"1026966","1026992","1027423","1028214","1028626","7074294","7647654","1029077")
          |AND cul.isclick>0 AND cul.adsrc=1
        """.stripMargin.format(argDay, argDay, argHour, argHour))
      .rdd
      .map {
        x =>
          val mediaId = x.getString(1)
          val adslotId = x.getString(2)
          val adslotType = x.getInt(3)
          var traceType = x.getString(4)
          var traceOp1 = x.getString(5)
          var total = 1.toLong
          ((adslotId, traceType, traceOp1), (Info(mediaId, adslotId, adslotType, 0, 0, 0, 0, traceType, traceOp1, total)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.mediaId, a.adslotId, a.adslotType, 0, 0, 0, 0, a.traceType, a.traceOp1, a.total + b.total))
      }
      .map(_._2)
      .cache()
    //println("jsTraceDataByWzNoSdk count is", jsTraceDataByWzNoSdk.count())


    /**
      * 获取网赚sdk流量信息
      */
    //获取网赚sdk流量信息
    val unionLogDataByWzSdk = ctx
      .sql(
        """
          |SELECT media_appsid,adslotid,adslot_type,isfill,isshow,isclick
          |FROM dl_cpc.cpc_union_log cul
          |WHERE cul.date="%s" AND cul.hour="%s"
          |AND cul.ext["client_type"].string_value="NATIVESDK"
          |AND cul.ext["adclass"].int_value=110110100 AND cul.adsrc=1
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val mediaId = x.getString(0)
          val adslotId = x.getString(1)
          val adslotType = x.getInt(2)
          val isfill = x.get(3).toString.toLong
          val isshow = x.get(4).toString.toLong
          val isclick = x.get(5).toString.toLong
          val req = 1.toLong
          ((adslotId), (Info(mediaId, adslotId, adslotType, req, isfill, isshow, isclick)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.mediaId, a.adslotId, a.adslotType, a.req + b.req, a.isfill + b.isfill, a.isshow + b.isshow, a.isclick + b.isclick))
      }
      .map(_._2)
      .cache()
    //println("unionLogDataByWzSdk count is", unionLogDataByWzSdk.count())

    //获取网赚sdk流量信息
    val jsTraceDataByWzSdk = ctx
      .sql(
        """
          |SELECT DISTINCT cutl.searchid,cul.media_appsid,cul.adslotid,cul.adslot_type,
          |cutl.trace_type,cutl.trace_op1
          |FROM dl_cpc.cpc_union_trace_log cutl
          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
          |WHERE cutl.date="%s" AND cul.date="%s"
          |AND cutl.hour="%s" AND cul.hour="%s"
          |AND cutl.trace_type in("lpload","load","active5","disactive","active_auto")
          |AND cul.ext["client_type"].string_value="NATIVESDK"
          |AND cul.ext["adclass"].int_value=110110100
          |AND cul.isclick>0 AND cul.adsrc=1
        """.stripMargin.format(argDay, argDay, argHour, argHour))
      .rdd
      .map {
        x =>
          val mediaId = x.getString(1)
          val adslotId = x.getString(2)
          val adslotType = x.getInt(3)
          var traceType = x.getString(4)
          var traceOp1 = x.getString(5)
          if (traceType == "load" || traceType == "active5" || traceType == "active_auto" || traceType == "disactive") {
            traceOp1 = ""
          }
          var total = 1.toLong
          ((adslotId, traceType, traceOp1), (Info(mediaId, adslotId, adslotType, 0, 0, 0, 0, traceType, traceOp1, total)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.mediaId, a.adslotId, a.adslotType, 0, 0, 0, 0, a.traceType, a.traceOp1, a.total + b.total))
      }
      .map(_._2)
      .cache()
    //println("jsTraceDataByWzSdk count is", jsTraceDataByWzSdk.count())


    /**
      * 获取sdk下载流量信息
      */

    //获取sdk下载流量信息
    val unionLogDataByDownSdk = ctx
      .sql(
        """
          |SELECT media_appsid,adslotid,adslot_type,isfill,isshow,isclick
          |FROM dl_cpc.cpc_union_log cul
          |WHERE cul.date="%s" AND cul.hour="%s"
          |AND cul.ext["client_type"].string_value="NATIVESDK"
          |AND cul.interaction=2 AND cul.adsrc=1
        """.stripMargin.format(argDay, argHour))
      .rdd
      .map {
        x =>
          val mediaId = x.getString(0)
          val adslotId = x.getString(1)
          val adslotType = x.getInt(2)
          val isfill = x.get(3).toString.toLong
          val isshow = x.get(4).toString.toLong
          val isclick = x.get(5).toString.toLong
          val req = 1.toLong
          ((adslotId), (Info(mediaId, adslotId, adslotType, req, isfill, isshow, isclick)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.mediaId, a.adslotId, a.adslotType, a.req + b.req, a.isfill + b.isfill, a.isshow + b.isshow, a.isclick + b.isclick))
      }
      .map(_._2)
      .cache()
    //println("unionLogDataByDownSdk count is", unionLogDataByDownSdk.count())

    //获取sdk下载流量信息
    val jsTraceDataByDownSdk = ctx
      .sql(
        """
          |SELECT DISTINCT cutl.searchid,cul.media_appsid,cul.adslotid,cul.adslot_type,
          |cutl.trace_type,cutl.trace_op1
          |FROM dl_cpc.cpc_union_trace_log cutl
          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
          |WHERE cutl.date="%s" AND cul.date="%s"
          |AND cutl.hour="%s" AND cul.hour="%s" AND cutl.trace_type in("apkdown")
          |AND cul.ext["client_type"].string_value="NATIVESDK"
          |AND cul.interaction=2
          |AND cul.isclick>0 AND cul.adsrc=1
        """.stripMargin.format(argDay, argDay, argHour, argHour))
      .rdd
      .map {
        x =>
          val mediaId = x.getString(1)
          val adslotId = x.getString(2)
          val adslotType = x.getInt(3)
          var traceType = x.getString(4)
          var traceOp1 = x.getString(5)
          var total = 1.toLong
          ((adslotId, traceType, traceOp1), (Info(mediaId, adslotId, adslotType, 0, 0, 0, 0, traceType, traceOp1, total)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.mediaId, a.adslotId, a.adslotType, 0, 0, 0, 0, a.traceType, a.traceOp1, a.total + b.total))
      }
      .map(_._2)
      .cache()
    //println("jsTraceDataByDownSdk count is", jsTraceDataByDownSdk.count())


    //获取详情页网赚sdk流量信息
    val jsTraceDataByType2WzSdk = ctx
      .sql(
        """
          |SELECT DISTINCT cutl.searchid,cul.media_appsid,cul.adslotid,cul.adslot_type,
          |cutl.trace_type,cutl.trace_op1
          |FROM dl_cpc.cpc_union_trace_log cutl
          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
          |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.hour="%s" AND cul.hour="%s"
          |AND cutl.trace_type in("lpload")
          |AND cul.ext["adclass"].int_value=110110100
          |AND cul.isclick>0 AND cul.adsrc=1
          |AND cul.ext["client_type"].string_value="JSSDK"
          |AND cul.media_appsid in ("80000001","80000002", "80000006", "800000062", "80000064", "80000066", "80000141")
          |-- AND (cul.adslot_type=2 OR cul.adslotid in("7568984","7412553","7167188","7338626","7648822","7867058","7189096","7443868"))
        """.stripMargin.format(argDay, argDay, argHour, argHour))
      .rdd
      .map {
        x =>
          val mediaId = x.getString(1)
          val adslotId = x.getString(2)
          val adslotType = x.getInt(3)
          var traceType = x.getString(4)
          var traceOp1 = x.getString(5)
          var total = 1.toLong
          ((adslotId, traceType, traceOp1), (Info(mediaId, adslotId, adslotType, 0, 0, 0, 0, traceType, traceOp1, total)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.mediaId, a.adslotId, a.adslotType, 0, 0, 0, 0, a.traceType, a.traceOp1, a.total + b.total))
      }
      .map(_._2)
      .cache()
    println("jsTraceDataByType2WzSdk count is", jsTraceDataByType2WzSdk.count())


    //获取详情页sdk下载流量信息
    val jsTraceDataByType2DownSdk = ctx
      .sql(
        """
          |SELECT DISTINCT cutl.searchid,cul.media_appsid,cul.adslotid,cul.adslot_type,
          |cutl.trace_type,cutl.trace_op1
          |FROM dl_cpc.cpc_union_trace_log cutl
          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
          |WHERE cutl.date="%s" AND cul.date="%s" AND cutl.hour="%s" AND cul.hour="%s"
          |AND cutl.trace_type in("apkdown") AND cul.interaction=2
          |AND cul.isclick>0 AND cul.adsrc=1
          |AND cul.ext["client_type"].string_value="JSSDK"
          |AND cul.media_appsid in ("80000001","80000002", "80000006", "800000062", "80000064", "80000066", "80000141")
          |-- AND cul.adslot_type=2 OR cul.adslotid in("7568984","7412553","7167188","7338626","7648822","7867058","7189096","7443868"))
        """.stripMargin.format(argDay, argDay, argHour, argHour))
      .rdd
      .map {
        x =>
          val mediaId = x.getString(1)
          val adslotId = x.getString(2)
          val adslotType = x.getInt(3)
          var traceType = x.getString(4)
          var traceOp1 = x.getString(5)
          var total = 1.toLong
          ((adslotId, traceType, traceOp1), (Info(mediaId, adslotId, adslotType, 0, 0, 0, 0, traceType, traceOp1, total)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.mediaId, a.adslotId, a.adslotType, 0, 0, 0, 0, a.traceType, a.traceOp1, a.total + b.total))
      }
      .map(_._2)
      .cache()
    println("jsTraceDataByType2DownSdk count is", jsTraceDataByType2DownSdk.count())

    /**
      * 非sdk全流量
      */
    var allNoSdkReq = unionLogDataByAllNoSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "request", x.traceOp1, x.req, argDay, argHour.toInt, "AllNoSdk")
      }

    var allNoSdkFill = unionLogDataByAllNoSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "served_request", x.traceOp1, x.isfill, argDay, argHour.toInt, "AllNoSdk")
      }

    var allNoSdkShow = unionLogDataByAllNoSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "impression", x.traceOp1, x.isshow, argDay, argHour.toInt, "AllNoSdk")
      }

    var allNoSdkClick = unionLogDataByAllNoSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "click", x.traceOp1, x.isclick, argDay, argHour.toInt, "AllNoSdk")
      }

    /**
      * sdk全流量
      */
    var allSdkReq = unionLogDataByAllSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "request", x.traceOp1, x.req, argDay, argHour.toInt, "AllSdk")
      }

    var allSdkFill = unionLogDataByAllSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "served_request", x.traceOp1, x.isfill, argDay, argHour.toInt, "AllSdk")
      }

    var allSdkShow = unionLogDataByAllSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "impression", x.traceOp1, x.isshow, argDay, argHour.toInt, "AllSdk")
      }

    var allSdkClick = unionLogDataByAllSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "click", x.traceOp1, x.isclick, argDay, argHour.toInt, "AllSdk")
      }

    /**
      * 网赚非sdk流量
      */
    //    var wzNoSdkReq = unionLogDataByWzNoSdk
    //      .map {
    //        x =>
    //          (x.mediaId, x.adslotId, x.adslotType, "request", x.traceOp1, x.req, argDay, argHour.toInt, "WzNoSdk")
    //      }

    var wzNoSdkFill = unionLogDataByWzNoSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "served_request", x.traceOp1, x.isfill, argDay, argHour.toInt, "WzNoSdk")
      }

    var wzNoSdkShow = unionLogDataByWzNoSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "impression", x.traceOp1, x.isshow, argDay, argHour.toInt, "WzNoSdk")
      }

    var wzNoSdkClick = unionLogDataByWzNoSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "click", x.traceOp1, x.isclick, argDay, argHour.toInt, "WzNoSdk")
      }

    var wzNoSdkLoad = jsTraceDataByWzNoSdk
      .map {
        x =>
          //"load"
          (x.mediaId, x.adslotId, x.adslotType, x.traceType, x.traceOp1, x.total, argDay, argHour.toInt, "WzNoSdk")
      }


    /**
      * 网赚sdk流量
      */
    //    var wzSdkReq = unionLogDataByWzSdk
    //      .map {
    //        x =>
    //          (x.mediaId, x.adslotId, x.adslotType, "request", x.traceOp1, x.req, argDay, argHour.toInt, "WzSdk")
    //      }

    var wzSdkFill = unionLogDataByWzSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "served_request", x.traceOp1, x.isfill, argDay, argHour.toInt, "WzSdk")
      }

    var wzSdkShow = unionLogDataByWzSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "impression", x.traceOp1, x.isshow, argDay, argHour.toInt, "WzSdk")
      }

    var wzSdkClick = unionLogDataByWzSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "click", x.traceOp1, x.isclick, argDay, argHour.toInt, "WzSdk")
      }

    var wzSdkLoad = jsTraceDataByWzSdk
      .filter(_.traceType != "lpload")
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, x.traceType, x.traceOp1, x.total, argDay, argHour.toInt, "WzSdk")
      }

    var wzSdkLpload = jsTraceDataByWzSdk
      .filter(_.traceType == "lpload")
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "lpload", x.traceOp1, x.total, argDay, argHour.toInt, "WzSdk")
      }


    /**
      * 下载sdk流量
      */
    //    var downSdkReq = unionLogDataByDownSdk
    //      .map {
    //        x =>
    //          (x.mediaId, x.adslotId, x.adslotType, "request", x.traceOp1, x.req, argDay, argHour.toInt, "DownSdk")
    //      }

    var downSdkFill = unionLogDataByDownSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "served_request", x.traceOp1, x.isfill, argDay, argHour.toInt, "DownSdk")
      }

    var downSdkShow = unionLogDataByDownSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "impression", x.traceOp1, x.isshow, argDay, argHour.toInt, "DownSdk")
      }

    var downSdkClick = unionLogDataByDownSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "click", x.traceOp1, x.isclick, argDay, argHour.toInt, "DownSdk")
      }

    var downSdkApkdown = jsTraceDataByDownSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "apkdown", x.traceOp1, x.total, argDay, argHour.toInt, "DownSdk")
      }

    var wzSdkType2Lpload = jsTraceDataByType2WzSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "lpload", x.traceOp1, x.total, argDay, argHour.toInt, "WzSdkAdType2")
      }


    var downSdkType2Apkdown = jsTraceDataByType2DownSdk
      .map {
        x =>
          (x.mediaId, x.adslotId, x.adslotType, "apkdown", x.traceOp1, x.total, argDay, argHour.toInt, "DownSdkAdType2")
      }

    var allData = allSdkReq.union(allSdkFill)
    allData = allData.union(allSdkShow)
    allData = allData.union(allSdkClick)
    allData = allData.union(allNoSdkReq)
    allData = allData.union(allNoSdkFill)
    allData = allData.union(allNoSdkShow)
    allData = allData.union(allNoSdkClick).repartition(50)

    allData = allData.union(wzNoSdkFill)
    allData = allData.union(wzNoSdkShow)
    allData = allData.union(wzNoSdkClick)
    allData = allData.union(wzNoSdkLoad)
    allData = allData.union(wzSdkFill)
    allData = allData.union(wzSdkShow)
    allData = allData.union(wzSdkClick)
    allData = allData.union(wzSdkLoad)
    allData = allData.union(wzSdkLpload).repartition(50)

    allData = allData.union(downSdkFill)
    allData = allData.union(downSdkShow)
    allData = allData.union(downSdkClick)
    allData = allData.union(downSdkApkdown).repartition(50)

    allData = allData.union(wzSdkType2Lpload)
    allData = allData.union(downSdkType2Apkdown)



    //    var allData = wzSdkType2Lpload.union(downSdkType2Apkdown)
    //    var allData = allSdkReq
    //      .union(allSdkFill)
    //      .union(allSdkShow)
    //      .union(allSdkClick)
    //      .union(allNoSdkReq)
    //      .union(allNoSdkFill)
    //      .union(allNoSdkShow)
    //      .union(allNoSdkClick)
    //      .union(wzNoSdkFill)
    //      .union(wzNoSdkShow)
    //      .union(wzNoSdkClick)
    //      .union(wzNoSdkLoad)
    //      .union(wzSdkFill)
    //      .union(wzSdkShow)
    //      .union(wzSdkClick)
    //      .union(wzSdkLoad)
    //      .union(wzSdkLpload)
    //      .union(downSdkFill)
    //      .union(downSdkShow)
    //      .union(downSdkClick)
    //      .union(downSdkApkdown)
    //      .union(wzSdkType2Lpload)
    //      .union(downSdkType2Apkdown)

    val allDataFrame =
      ctx.createDataFrame(allData.filter(_._5.length < 200))
        .toDF("media_id", "adslot_id", "adslot_type", "target_type", "trace_op1", "target_value", "date", "hour", "data_type")
    //
    allDataFrame.show(10)
    //
    clearReportSdkTrace(argDay, argHour)

    allDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_sdk_trace", mariadbProp)
    println("InsertReportSdkTrace_done")


    //---------------------------------------------------

    //---------------------------------------------------

    //---------------------------------------------------

  }

  def clearReportSdkTrace(date: String, hour: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.report_sdk_trace where `date` = "%s" and `hour` = %d
        """.stripMargin.format(date, hour.toInt)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }

}
