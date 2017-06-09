package com.cpc.spark.log.report

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.UnionLog
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by Roy on 2017/4/26.
  */
object GetHourReport {

  val mariadbUrl = "jdbc:mysql://10.9.180.16:3306/report"

  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: GetHourReport <hive_table> <hour_before>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val table = args(0)
    val hourBefore = args(1).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)

    mariadbProp.put("user", "report")
    mariadbProp.put("password", "report!@#")
    mariadbProp.put("driver", "org.mariadb.jdbc.Driver")

    val ctx = SparkSession.builder()
      .appName("cpc get hour report from %s %s/%s".format(table, date, hour))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._


    val unionLog = ctx.sql(
      s"""
         |select * from dl_cpc.%s where `date` = "%s" and `hour` = "%s" and isfill = 1 and adslotid > 0
       """.stripMargin.format(table, date, hour))
      .as[UnionLog]
      .rdd.cache()

    //write hourly data to mysql
    val chargeData = unionLog
      .map {
        x =>
          val charge = MediaChargeReport(
            media_id = x.media_appsid.toInt,
            adslot_id = x.adslotid.toInt,
            unit_id = x.unitid,
            idea_id = x.ideaid,
            plan_id = x.planid,
            adslot_type = x.adslot_type,
            user_id = x.userid,
            request = 1,
            served_request = x.isfill,
            impression = x.isshow,
            click = x.isclick,
            charged_click = x.isCharged(),
            spam_click = x.isSpamClick(),
            cash_cost = x.realCost(),
            date = x.date,
            hour = x.hour.toInt
          )
          (charge.key, charge)
      }
      .reduceByKey((x, y) => x.sum(y))
      .map(_._2)


    clearReportHourData("report_media_charge_hourly", date, hour)
    ctx.createDataFrame(chargeData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_charge_hourly", mariadbProp)

    val geoData = unionLog
      .map {
        x =>
          val report = MediaGeoReport(
            media_id = x.media_appsid.toInt,
            adslot_id = x.adslotid.toInt,
            unit_id = x.unitid,
            idea_id = x.ideaid,
            plan_id = x.planid,
            adslot_type = x.adslot_type,
            user_id = x.userid,
            country = x.country,
            province = x.province,
            //city = x.city,
            request = 1,
            served_request = x.isfill,
            impression = x.isshow,
            click = x.isclick,
            charged_click = x.isCharged(),
            spam_click = x.isSpamClick(),
            cash_cost = x.realCost(),
            date = x.date,
            hour = x.hour.toInt
          )
          (report.key, report)
      }
      .reduceByKey((x, y) => x.sum(y))
      .map(_._2)

    clearReportHourData("report_media_geo_hourly", date, hour)
    ctx.createDataFrame(geoData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_geo_hourly", mariadbProp)

    val osData = unionLog
      .map {
        x =>
          val report = MediaOsReport(
            media_id = x.media_appsid.toInt,
            adslot_id = x.adslotid.toInt,
            unit_id = x.unitid,
            idea_id = x.ideaid,
            plan_id = x.planid,
            adslot_type = x.adslot_type,
            user_id = x.userid,
            os_type = x.os,
            request = 1,
            served_request = x.isfill,
            impression = x.isshow,
            click = x.isclick,
            charged_click = x.isCharged(),
            spam_click = x.isSpamClick(),
            cash_cost = x.realCost(),
            date = x.date,
            hour = x.hour.toInt
          )
          (report.key, report)
      }
      .reduceByKey((x, y) => x.sum(y))
      .map(_._2)

    clearReportHourData("report_media_os_hourly", date, hour)
    ctx.createDataFrame(osData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_os_hourly", mariadbProp)

    unionLog.unpersist()

    val fillLog = ctx.sql(
      s"""
         |select * from dl_cpc.%s where `date` = "%s" and `hour` = "%s" and adslotid > 0
       """.stripMargin.format(table, date, hour))
      .as[UnionLog]
      .rdd.cache()

    val fillData = fillLog
      .map {
        x =>
          val report = MediaFillReport(
            media_id = x.media_appsid.toInt,
            adslot_id = x.adslotid.toInt,
            adslot_type = x.adslot_type,
            request = 1,
            served_request = x.isfill,
            impression = x.isshow,
            click = x.isclick,
            charged_click = x.isCharged(),
            spam_click = x.isSpamClick(),
            cash_cost = x.realCost(),
            date = x.date,
            hour = x.hour.toInt
          )
          (report.key, report)
      }
      .reduceByKey((x, y) => x.sum(y))
      .map(_._2)

    clearReportHourData("report_media_fill_hourly", date, hour)
    ctx.createDataFrame(fillData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_fill_hourly", mariadbProp)

    ctx.stop()
  }

  def clearReportHourData(tbl: String, date: String, hour: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"));
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"));
      val stmt = conn.createStatement();
      val sql =
        """
          |delete from report.%s where `date` = "%s" and `hour` = %d
        """.stripMargin.format(tbl, date, hour.toInt);
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
