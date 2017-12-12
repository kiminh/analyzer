package com.cpc.spark.log.report

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by Roy on 2017/4/26.
  */
object GetHourReport {

  var mariadbUrl = ""

  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: GetHourReport <hive_table> <date:string> <hour:string>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val table = args(0)
    val date = args(1)
    val hour = args(2)
    //val hourBefore = args(1).toInt
    //val cal = Calendar.getInstance()
    //cal.add(Calendar.HOUR, -hourBefore)
    //val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    //val hour = new SimpleDateFormat("HH").format(cal.getTime)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password",conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))
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
   val ctrData = unionLog
      //.filter(_.ext.getOrElse("rank_discount", ExtValue()).int_value == 100)
      .map{
        u =>
          val exptag = u.exptags.split(",").find(_.startsWith("ctrmodel")).getOrElse("base")
          var expctr = 0
          if (u.isshow > 0 && u.ext != null) {
            val v = u.ext.getOrElse("exp_ctr", null)
            if (v != null) {
              expctr = v.int_value
            }
          }

          val discount = u.ext.getOrElse("rank_discount", ExtValue()).int_value
          var cost = u.realCost().toFloat
          if (discount > 0) {
            cost = cost * discount.toFloat / 100
          }

          val ctr = CtrReport(
            media_id = u.media_appsid.toInt,
            adslot_id = u.adslotid.toInt,
            adslot_type = u.adslot_type,
            //unit_id = u.unitid,
            //idea_id = u.ideaid,
            //plan_id = u.planid,
            //user_id = u.userid,
            exp_tag = exptag,
            request = 1,
            served_request = u.isfill,
            impression = u.isshow,
            cash_cost = cost,
            click = u.isclick,
            exp_click = expctr,
            date = "%s %s:00:00".format(u.date, u.hour)
          )

          val key = (ctr.media_id, ctr.adslot_id, ctr.plan_id, ctr.unit_id, ctr.idea_id, exptag)
          (key, ctr)
      }
      .reduceByKey {
        (x, y) =>
          x.copy(
            request = x.request + y.request,
            served_request = x.served_request + y.served_request,
            impression = x.impression + y.impression,
            cash_cost = x.cash_cost + y.cash_cost,
            click = x.click + y.click,
            exp_click = x.exp_click + y.exp_click
          )
      }
      .map {
        x =>
          val ctr = x._2.copy(
            exp_click = x._2.exp_click / 1000000
          )
          if (ctr.impression > 0) {
            ctr.copy(
              ctr = ctr.click.toFloat / ctr.impression.toFloat,
              exp_ctr = ctr.exp_click / ctr.impression.toFloat,
              cpm = ctr.cash_cost / ctr.impression.toFloat * (1000 / 100),
              cash_cost = ctr.cash_cost.toInt
            )
          } else {
            ctr.copy(
              cash_cost = ctr.cash_cost.toInt
            )
          }
      }

    clearReportHourData("report_ctr_prediction_hourly", "%s %s:00:00".format(date, hour), "0")
    ctx.createDataFrame(ctrData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_ctr_prediction_hourly", mariadbProp)
    println("ctr", ctrData.count())

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
            click = x.isclick + x.spamClick(),
            charged_click = x.isclick,
            spam_click = x.spamClick(),
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

    println("charge", chargeData.count())

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
            click = x.isclick + x.spamClick(),
            charged_click = x.isclick,
            spam_click = x.spamClick(),
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
    println("geo", geoData.count())

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
            click = x.isclick + x.spamClick(),
            charged_click = x.isclick,
            spam_click = x.spamClick(),
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
    println("os", osData.count())
    val ipRequestData = unionLog.filter(x => x.isshow >0)
      .map {
        x =>
          ((x.media_appsid.toInt,x.adslotid.toInt,x.adslot_type,x.ip,x.date,x.hour.toInt), 1)
      }.reduceByKey((x,y) => x+y).map{
      case ((media_appsid, adslotid, adslot_type, ip, date2, hour2), count) =>
        ((media_appsid, adslotid, adslot_type, count, date2, hour2), 1)
    }.reduceByKey((x,y) => x+y).map{
      case ((media_appsid, adslotid, adslot_type, ip_num, date2, hour2), count) =>
        val report = MediaIpReport(
          media_id = media_appsid,
          adslot_id = adslotid,
          adslot_type = adslot_type,
          num = ip_num,
          count= count,
          date = date2,
          hour = hour2
        )
        report
    }
    clearReportHourData("report_media_ip_request_hourly", date, hour)
    ctx.createDataFrame(ipRequestData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_ip_request_hourly", mariadbProp)
    println("ip_request", ipRequestData.count())

    val ipClickData = unionLog.filter(x => x.isclick >0)
      .map {
        x =>
          ((x.media_appsid.toInt,x.adslotid.toInt,x.adslot_type,x.ip,x.date,x.hour.toInt), 1)
      }.reduceByKey((x,y) => x+y).map{
      case ((media_appsid, adslotid, adslot_type, ip, date2, hour2), count) =>
        ((media_appsid, adslotid, adslot_type, count, date2, hour2), 1)
    }.reduceByKey((x,y) => x+y).map{
      case ((media_appsid, adslotid, adslot_type, ip_num, date2, hour2), count) =>
        val report = MediaIpReport(
          media_id = media_appsid,
          adslot_id = adslotid,
          adslot_type = adslot_type,
          num = ip_num,
          count= count,
          date = date2,
          hour = hour2
        )
        report
    }
    clearReportHourData("report_media_ip_click_hourly", date, hour)
    ctx.createDataFrame(ipClickData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_ip_click_hourly", mariadbProp)
    println("ip_click", ipClickData.count())

    val uidRequestData = unionLog.filter(x => x.uid.length >0 && x.isshow >0)
      .map {
        x =>
          ((x.media_appsid.toInt,x.adslotid.toInt,x.adslot_type,x.uid,x.date,x.hour.toInt), 1)
      }.reduceByKey((x,y) => x+y).map{
      case ((media_appsid, adslotid, adslot_type, uid, date2, hour2), count) =>
        ((media_appsid, adslotid, adslot_type, count, date2, hour2), 1)
    }.reduceByKey((x,y) => x+y).map{
      case ((media_appsid, adslotid, adslot_type, uid_num, date2, hour2), count) =>
        val report = MediaIpReport(
          media_id = media_appsid,
          adslot_id = adslotid,
          adslot_type = adslot_type,
          num = uid_num,
          count= count,
          date = date2,
          hour = hour2
        )
        report
    }
    clearReportHourData("report_media_uid_request_hourly", date, hour)
    ctx.createDataFrame(uidRequestData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_uid_request_hourly", mariadbProp)
    println("uid_request", uidRequestData.count())

    val uidClickData = unionLog.filter(x => x.uid.length >0 && x.isclick > 0)
      .map {
        x =>
          ((x.media_appsid.toInt,x.adslotid.toInt,x.adslot_type,x.uid,x.date,x.hour.toInt), 1)
      }.reduceByKey((x,y) => x+y).map{
      case ((media_appsid, adslotid, adslot_type, uid, date2, hour2), count) =>
        ((media_appsid, adslotid, adslot_type, count, date2, hour2), 1)
    }.reduceByKey((x,y) => x+y).map{
      case ((media_appsid, adslotid, adslot_type, uid_num, date2, hour2), count) =>
        val report = MediaIpReport(
          media_id = media_appsid,
          adslot_id = adslotid,
          adslot_type = adslot_type,
          num = uid_num,
          count= count,
          date = date2,
          hour = hour2
        )
        report
    }
    clearReportHourData("report_media_uid_click_hourly", date, hour)
    ctx.createDataFrame(uidClickData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_uid_click_hourly", mariadbProp)
    println("uid_click", uidClickData.count())

    unionLog.unpersist()

    val fillLog = ctx.sql(
      s"""
         |select * from dl_cpc.%s where `date` = "%s" and `hour` = "%s" and adslotid > 0
       """.stripMargin.format(table, date, hour))
      .as[UnionLog]
      .rdd

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
            click = x.isclick + x.spamClick(),
            charged_click = x.isclick,
            spam_click = x.spamClick(),
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
    println("fill", fillData.count())
    ctx.stop()
  }

  def clearReportHourData(tbl: String, date: String, hour: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.%s where `date` = "%s" and `hour` = %d
        """.stripMargin.format(tbl, date, hour.toInt)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }

  private case class CtrReport(
                               media_id: Int = 0,
                               adslot_id: Int = 0,
                               adslot_type: Int = 0,
                               idea_id: Int = 0,
                               unit_id: Int = 0,
                               plan_id: Int = 0,
                               user_id: Int = 0,
                               exp_tag: String = "",
                               request: Int = 0,
                               served_request: Int = 0,
                               impression: Int = 0,
                               cash_cost: Float = 0,
                               click: Int = 0,
                               exp_click: Float = 0,
                               ctr: Float = 0,
                               exp_ctr: Float = 0,
                               cpm: Float = 0,
                               date: String = "",
                               hour: Int = 0
                             )

}
