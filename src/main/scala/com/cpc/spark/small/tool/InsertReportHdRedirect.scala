package com.cpc.spark.small.tool

import java.sql.DriverManager
import java.util.Properties

import com.cpc.spark.log.parser.CfgLog
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/8/4.
  */
object InsertReportHdRedirect {
  var mariadbUrl = ""
  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: day <date>
        """.stripMargin)
      System.exit(1)
    }
    val argDay = args(0).toString
    val argHour = args(1).toString
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession.builder()
      .appName("InsertHdRedirectLog date " + argDay + " ,hour " + argHour)
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    var cfgLog = ctx.read
      .parquet("/warehouse/dl_cpc.db/cpc_cfg_log/date=%s/hour=%s".format(argDay, argHour))
      .as[CfgLog].rdd.filter(x => x.log_type == "/hdjump" || x.log_type == "/reqhd")
      .cache()
    var toResult = cfgLog.map(x => ((x.aid, x.redirect_url, x.hour), 1)).reduceByKey((x, y) => x + y).map {
      case ((adslotId, url, hour), count) =>
        ((argDay, hour, adslotId),HdRedict(argDay, hour, adslotId, url, count))
    }
    .reduceByKey{
      (a,b)=>
        val date = a.date
        val hour  = a.hour
        val adslotId = a.adslot_id
        var pv = a.pv+b.pv
        HdRedict(date, hour, adslotId, "", pv)
    }
    .map{
      x=>
        (x._2.adslot_id,x._2.date,x._2.hour.toInt,x._2.pv)
    }

    println("count:" + toResult.count())

    val insertDataFrame =ctx.createDataFrame(toResult).toDF("adslot_id","date","hour","pv")

    insertDataFrame.show(10)

   clearReportHourData("report_hd_redirect_pv", argDay, argHour.toInt)

    insertDataFrame
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_hd_redirect_pv", mariadbProp)

    ctx.stop()
  }

  def clearReportHourData(tbl: String, date: String, hour: Int): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.%s where `date` = "%s" AND hour="%d"
        """.stripMargin.format(tbl, date, hour)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e)
    }
  }

  case class HdRedict(
                       date: String = "",
                       hour: String = "",
                       adslot_id: String = "",
                       redirect_url: String = "",
                       pv: Int = 0
                     )

}