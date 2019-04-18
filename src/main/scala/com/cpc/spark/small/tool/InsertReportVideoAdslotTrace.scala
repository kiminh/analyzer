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
          |SELECT adslot_id,adslot_type,adtype,usertype,sum(isshow),sum(isclick),
          |sum((CASE isclick
          |WHEN 1 THEN price
          |ELSE 0
          |END)) price,
          |count(DISTINCT userid),count(DISTINCT ideaid)
          |FROM dl_cpc.cpc_basedata_union_events
          |WHERE day="%s" AND adslot_id in("7268884","1018971","1024335","1025156") AND ideaid>0 AND adsrc=1 AND charge_type = 1
          |AND client_type="NATIVESDK"
          |GROUP BY adslot_id,adslot_type,adtype,usertype
        """.stripMargin.format(argDay)) //, argHour
      .rdd
      .map {
        x =>
          val adslotid = x.getAs[String](0)
          val adslot_type = x.getAs[Int](1)
          val adtype = x.getAs[Int](2)
          val usertype = x.getAs[Int](3)
          val isshow = x.getAs[Long](4)
          val isclick = x.getAs[Long](5)
          val price = x.getAs[Long](6)
          val userTotal = x.getAs[Long](7)
          val ideaTotal = x.getAs[Long](8)
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
          |SELECT DISTINCT cutl.searchid,cul.adslot_id,adslot_type,adtype,usertype,cutl.trace_type,cutl.trace_op1
          |FROM dl_cpc.cpc_basedata_trace_event cutl
          |INNER JOIN dl_cpc.cpc_basedata_union_events cul ON cutl.searchid=cul.searchid
          |WHERE cutl.day="%s" AND cul.day="%s"
          |AND cul.client_type="NATIVESDK"
          |AND cul.adslot_id in("7268884","1018971","1024335","1025156") AND charge_type = 1
          |AND cul.isclick>0 AND cul.adsrc=1 AND cutl.trace_type in("lpload","apkdown")
        """.stripMargin.format(argDay, argDay)) //, argHour, argHour
      .rdd
      .map {
        x =>
          val adslotid = x.getAs[String](1)
          val adslot_type = x.getAs[Int](2)
          val adtype = x.getAs[Int](3)
          val usertype = x.getAs[Int](4)
          var traceType = x.getAs[String](5)
          var traceOp1 = x.getAs[String](6)
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
