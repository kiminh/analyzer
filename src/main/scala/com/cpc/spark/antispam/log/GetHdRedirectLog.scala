package com.cpc.spark.antispam.log

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.{CfgLog}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/8/4.
  */
object GetHdRedirectLog {
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
    val dayBefore = args(0).toInt
    Logger.getRootLogger.setLevel(Level.WARN)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.union_write.url")
    mariadbProp.put("user", conf.getString("mariadb.union_write.user"))
    mariadbProp.put("password", conf.getString("mariadb.union_write.password"))
    mariadbProp.put("driver", conf.getString("mariadb.union_write.driver"))

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val ctx = SparkSession.builder()
      .appName("model user anal" + date)
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    var sql1 = (" SELECT * FROM dl_cpc.cpc_cfg_log where `date`='%s' and log_type in('/hdjump','/reqhd') ").format(date)

    println("sql1:" + sql1)
    var cfgLog = ctx.sql(sql1).as[CfgLog]
      .rdd.cache()
    var toResult = cfgLog.map(x => ((x.aid,x.redirect_url,x.hour), 1)).reduceByKey((x,y) => x+y).map{
      case ((adslotId, url,hour),count) =>
        HdRedict(date,hour, adslotId,url,count)
    }
   println("count:" + cfgLog.count())
    clearReportHourData("report_hd_redirect", date)
    ctx.createDataFrame(toResult)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "union.report_hd_redirect", mariadbProp)
    ctx.stop()
  }

  def clearReportHourData(tbl: String, date: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from union.%s where `date` = "%s"
        """.stripMargin.format(tbl, date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e)
    }
  }

  case class HdRedict(
                          date: String = "",
                          hour: String = "",
                          adslot_id:String = "",
                          redirect_url: String = "",
                          pv: Int = 0
                        )

}