package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2018/3/14.
  */
object InsertReportVideoAdslotTrace {

  case class Info(
                   adslotid: String = "",
                   adslot_type: Int = 0,
                   adtype: Int = 0,
                   account_type: Int = 0,
                   traceType: String = "",
                   traceOp1: String = "",
                   total: Long = 0,
                   date: String = "") {
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
      .appName("InsertReportSdkVersionTrace is run day is %s".format(argDay))
      .enableHiveSupport()
      .getOrCreate()

    println("InsertReportVideoAdslotTrace is run day is %s".format(argDay))


    val unionLogDataByAll = ctx
      .sql(
        """
          |SELECT adslotid,adslot_type,adtype,ext["usertype"].int_value,sum(isshow),sum(isclick),
          |sum((CASE isclick
          |WHEN 1 THEN price
          |ELSE 0
          |END)) price,
          |count(DISTINCT userid),count(DISTINCT ideaid)
          |FROM dl_cpc.cpc_union_log
          |WHERE `date`="%s" AND adslotid in("7268884","1018971","1024335","1025156") AND ideaid>0 AND adsrc=1 AND ext["charge_type"].int_value = 1
          |AND ext["client_type"].string_value="NATIVESDK"
          |GROUP BY adslotid,adslot_type,adtype,ext["usertype"].int_value
        """.stripMargin.format(argDay)) //, argHour
      .rdd
      .map {
        x =>
          val adslotid = x.getString(0)
          val adslot_type = x.get(1).toString.toInt
          val adtype = x.get(2).toString.toInt
          val usertype = x.get(3).toString.toInt
          val isshow = x.get(4).toString.toLong
          val isclick = x.get(5).toString.toLong
          val price = x.get(6).toString.toLong
          val userTotal = x.get(7).toString.toLong
          val ideaTotal = x.get(8).toString.toLong
          (adslotid, adslot_type, adtype, usertype, isshow, isclick, price, userTotal, ideaTotal)
      }
      .cache()
    println("unionLogDataByAll count is", unionLogDataByAll.count())


    val impDataByAll = unionLogDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, x._4, "impression", "", x._5, argDay)
      }

    var unionLogDataByAllx = impDataByAll

    println(unionLogDataByAllx.count())

    val clickDataByAll = unionLogDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, x._4, "click", "", x._6, argDay)
      }

    unionLogDataByAllx = unionLogDataByAllx.union(clickDataByAll)

    val priceDataByAll = unionLogDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, x._4, "price", "", x._7, argDay)
      }

    unionLogDataByAllx = unionLogDataByAllx.union(priceDataByAll)

    val userTotalDataByAll = unionLogDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, x._4, "user_total", "", x._8, argDay)
      }

    unionLogDataByAllx = unionLogDataByAllx.union(userTotalDataByAll)

    val ideaTotalDataByAll = unionLogDataByAll
      .map {
        x =>
          Info(x._1, x._2, x._3, x._4, "idea_total", "", x._9, argDay)
      }

    unionLogDataByAllx = unionLogDataByAllx.union(ideaTotalDataByAll)

    println(unionLogDataByAllx.count())

    //获取trace信息
    val traceDataByAll = ctx
      .sql(
        """
          |SELECT DISTINCT cutl.searchid,adslotid,adslot_type,adtype,ext["usertype"].int_value,cutl.trace_type,cutl.trace_op1
          |FROM dl_cpc.cpc_union_trace_log cutl
          |INNER JOIN dl_cpc.cpc_union_log cul ON cutl.searchid=cul.searchid
          |WHERE cutl.date="%s" AND cul.date="%s"
          |AND cul.ext["client_type"].string_value="NATIVESDK"
          |AND cul.adslotid in("7268884","1018971","1024335","1025156") AND ext["charge_type"].int_value = 1
          |AND cul.isclick>0 AND cul.adsrc=1 AND cutl.trace_type in("lpload","apkdown")
        """.stripMargin.format(argDay, argDay)) //, argHour, argHour
      .rdd
      .map {
        x =>
          val adslotid = x.getString(1)
          val adslot_type = x.get(2).toString.toInt
          val adtype = x.get(3).toString.toInt
          val usertype = x.get(4).toString.toInt
          var traceType = x.getString(5)
          var traceOp1 = x.getString(6)
          var total = 1.toLong
          ((adslotid, adslot_type, adtype, usertype, traceType, traceOp1),
            (Info(adslotid, adslot_type, adtype, usertype, traceType, traceOp1, total, argDay)))
      }
      .reduceByKey {
        (a, b) =>
          (Info(a.adslotid, a.adslot_type, a.adtype, a.account_type, a.traceType, a.traceOp1, a.total + b.total, argDay))
      }
      .map(_._2)
      .filter {
        x =>
          x.traceType.length <= 250 && x.traceOp1.length <= 250
      }
      .cache()
    println("traceDataByAll count is", traceDataByAll.count())


    val allDataByAll = unionLogDataByAllx
      .union(traceDataByAll)
      .map {
        x =>
          (x.adslotid, x.adslot_type, x.adtype, x.account_type, x.traceType, x.traceOp1, x.total, x.date)
      }


    val allDataFrame =
      ctx.createDataFrame(allDataByAll)
        .toDF("adslot_id", "adslot_type", "adtype", "account_type", "target_type", "trace_op1", "target_value", "date")

    println("allDataFrame count is", allDataFrame.count())

    clearReportData(argDay)

    allDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariaReport2dbUrl, "report2.report_video_adslot_trace", mariaReport2dbProp)
    println("InsertReportVideoAdslotTrace_done")


    //---------------------------------------------------

    //---------------------------------------------------

    //---------------------------------------------------

  }

  def clearReportData(date: String): Unit = {
    try {
      Class.forName(mariaReport2dbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariaReport2dbUrl,
        mariaReport2dbProp.getProperty("user"),
        mariaReport2dbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report2.report_video_adslot_trace where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }

}
