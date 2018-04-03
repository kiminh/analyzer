package com.cpc.spark.antispam.log

import java.sql.DriverManager
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by wanli on 2017/8/4.
  */
object GetAntispamLog {
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
    val date = args(0)
    Logger.getRootLogger.setLevel(Level.WARN)
    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession.builder()
      .appName("get antispamlog " + date)
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._
    var sql1 = (" SELECT hour,antispam_rules,media_appsid,adslotid,adslot_type,count(distinct searchid) as num  from dl_cpc.cpc_click_log " +
      "where `date` ='%s' and isclick=1 group by media_appsid,adslotid,adslot_type,antispam_rules,hour").format(date)
    println("sql1:" + sql1)
    var union = ctx.sql(sql1).rdd.map {
      x =>
        try {
          val hour = x(0).toString()
          var antispam_rules = x(1).toString()
          val media_appsid = x(2).toString().toInt
          val adslot_id = x(3).toString().toInt
          val adslot_type = x(4).toString().toInt
          val num = x(5).toString().toInt
          if(antispam_rules.length == 0){
            antispam_rules ="OK"
          }
          (hour, media_appsid, adslot_id, adslot_type, antispam_rules, num)
        }catch {
          case e: Exception =>
            null
        }
    }.filter(x => x!= null)
    var toResult = union.map(x => ((x._1, x._2, x._3, x._4, x._5), x._6)).reduceByKey((x,y) => x+y).map {
      case ((hour, media_appsid, adslot_id, adslot_type, antispam_rules), num) =>
        var antispam_rules_name = antispam_rules
        AntispamLog(
          date,
          hour,
          media_appsid,
          adslot_id,
          adslot_type,
          antispam_rules_name,
          num,
          0
        )
    }

    println("count:"+ toResult.count())
    clearReportHourData("report_media_antispam_hourly", date)
    ctx.createDataFrame(toResult)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_antispam_hourly", mariadbProp)
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
          |delete from report.%s where `date` = "%s"
        """.stripMargin.format(tbl, date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e)
    }
  }

  case class AntispamLog(
                          date: String = "",
                          hour: String = "",
                          media_id: Int = 0,
                          adslot_id: Int = 0,
                          adslot_type: Int = 0,
                          antispam_rules: String = "",
                          num: Int = 0,
                          total: Int = 0
                        )

}