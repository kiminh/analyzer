package com.cpc.spark.log.report

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.{ExtValue, TraceLog, UnionLog}
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


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
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))
    val ctx = SparkSession.builder()
      .appName("cpc get hour report from %s %s/%s".format(table, date, hour))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._


    val unionLog = ctx.sql(
      s"""
         |select *,
         |      ext['spam_click'].int_value as spam_click,
         |      ext['rank_discount'].int_value as rank_discount,
         |      ext['cvr_threshold'].int_value as cvr_threshold,
         |      ext['adclass'].int_value as adclass,
         |      ext['exp_cvr'].int_value as exp_cvr,
         |      ext['exp_ctr'].int_value as exp_ctr
         |from dl_cpc.%s where `date` = "%s" and `hour` = "%s" and isfill = 1 and adslotid > 0
       """.stripMargin.format(table, date, hour))
      //      .as[UnionLog]
      .rdd.cache()

    val chargeData = unionLog
      .map {
        x =>
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          var antispam_score = x.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = x.getAs[Int]("price")
          } else {
            realCost = 0
          }
          val charge = MediaChargeReport( //adslotType = x.getAs[Int]("adslot_type")
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslotid").toInt,
            unit_id = x.getAs[Int]("unitid"),
            idea_id = x.getAs[Int]("ideaid"),
            plan_id = x.getAs[Int]("planid"),
            adslot_type = x.getAs[Int]("adslot_type"),
            user_id = x.getAs[Int]("userid"),
            request = 1,
            served_request = x.getAs[Int]("isfill"),
            impression = x.getAs[Int]("isshow"),
            click = isclick + spam_click,
            charged_click = isclick,
            spam_click = spam_click,
            cash_cost = realCost,
            date = x.getAs[String]("date"),
            hour = x.getAs[String]("hour").toInt
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
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          var antispam_score = x.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = x.getAs[Int]("price")
          } else {
            realCost = 0
          }
          val report = MediaGeoReport(
            //media_id = x.media_appsid.toInt,
            //adslot_id = x.adslotid.toInt,
            unit_id = x.getAs[Int]("unitid"),
            idea_id = x.getAs[Int]("ideaid"),
            plan_id = x.getAs[Int]("planid"),
            adslot_type = x.getAs[Int]("adslot_type"),
            user_id = x.getAs[Int]("userid"),
            country = x.getAs[Int]("country"),
            province = x.getAs[Int]("province"),
            //city = x.city,
            request = 1,
            served_request = x.getAs[Int]("isfill"),
            impression = x.getAs[Int]("isshow"),
            click = isclick + spam_click,
            charged_click = isclick,
            spam_click = spam_click,
            cash_cost = realCost,
            date = x.getAs[String]("date"),
            hour = x.getAs[String]("hour").toInt
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
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          var antispam_score = x.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = x.getAs[Int]("price")
          } else {
            realCost = 0
          }
          val report = MediaOsReport(
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslotid").toInt,
            unit_id = x.getAs[Int]("unitid"),
            idea_id = x.getAs[Int]("ideaid"),
            plan_id = x.getAs[Int]("planid"),
            adslot_type = x.getAs[Int]("adslot_type"),
            user_id = x.getAs[Int]("userid"),
            os_type = x.getAs[Int]("os"),
            request = 1,
            served_request = x.getAs[Int]("isfill"),
            impression = x.getAs[Int]("isshow"),
            click = isclick + spam_click,
            charged_click = isclick,
            spam_click = spam_click,
            cash_cost = realCost,
            date = x.getAs[String]("date"),
            hour = x.getAs[String]("hour").toInt
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
    /* val ipRequestData = unionLog.filter(x => x.isshow >0)
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
 */
    val fillLog = ctx.sql(
      s"""
         |select *,
         |      ext['spam_click'].int_value as spam_click,
         |      ext['rank_discount'].int_value as rank_discount,
         |      ext['cvr_threshold'].int_value as cvr_threshold,
         |      ext['adclass'].int_value as adclass,
         |      ext['exp_cvr'].int_value as exp_cvr,
         |      ext['exp_ctr'].int_value as exp_ctr
         |      from dl_cpc.%s where `date` = "%s" and `hour` = "%s" and adslotid > 0
       """.stripMargin.format(table, date, hour))
      //      .as[UnionLog]
      .rdd

    val fillData = fillLog
      .map {
        x =>
          var isclick = x.getAs[Int]("isclick")
          var spam_click = x.getAs[Int]("spam_click")
          var antispam_score = x.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = x.getAs[Int]("price")
          } else {
            realCost = 0
          }
          val report = MediaFillReport(
            media_id = x.getAs[String]("media_appsid").toInt,
            adslot_id = x.getAs[String]("adslotid").toInt,
            adslot_type = x.getAs[Int]("adslot_type"),
            request = 1,
            served_request = x.getAs[Int]("isfill"),
            impression = x.getAs[Int]("isshow"),
            click = isclick + spam_click,
            charged_click = isclick,
            spam_click = spam_click,
            cash_cost = realCost,
            date = x.getAs[String]("date"),
            hour = x.getAs[String]("hour").toInt
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

    val ctrData = unionLog
      .filter(x => x.getAs[Int]("rank_discount") <= 20000 && x.getAs[Int]("ideaid") > 0)
      .map {
        u =>
          val exptag = u.getAs[String]("exptags").split(",").find(_.startsWith("ctrmodel")).getOrElse("base")
          var expctr = 0
          if (u.getAs[Int]("isshow") > 0) {
            expctr = u.getAs[Int]("exp_ctr")
          }

          val discount = u.getAs[Int]("rank_discount")

          var isclick = u.getAs[Int]("isclick")
          var spam_click = u.getAs[Int]("spam_click")
          var antispam_score = u.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = u.getAs[Int]("price")
          } else {
            realCost = 0
          }

          var cost = realCost.toFloat
          if (discount > 0) {
            cost = cost * discount.toFloat / 100
          }

          val ctr = CtrReport(
            media_id = u.getAs[String]("media_appsid").toInt,
            adslot_id = u.getAs[String]("adslotid").toInt,
            adslot_type = u.getAs[Int]("adslot_type"),
            //unit_id = u.unitid,
            //idea_id = u.ideaid,
            //plan_id = u.planid,
            //user_id = u.userid,
            exp_tag = exptag,
            request = 1,
            served_request = u.getAs[Int]("isfill"),
            impression = u.getAs[Int]("isshow"),
            cash_cost = cost,
            click = isclick,
            exp_click = expctr,
            date = "%s %s:00:00".format(u.getAs[String]("date"), u.getAs[String]("hour"))
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

    val cvrlog = ctx.sql(
      s"""
         |select * from dl_cpc.cpc_union_trace_log where `date` = "%s" and hour = "%s"
        """.stripMargin.format(date, hour))
      //      .as[TraceLog]
      .rdd
      .map {
        x =>
          (x.getAs[String]("searchid"), Seq(x))
      }
      .reduceByKey(_ ++ _)
      .map {
        x =>
          val convert = cvrPositive(x._2)
          (x._1, convert)
      }

    val cvrData = unionLog.filter(_.getAs[Int]("isclick") > 0)
      .map(x => (x.getAs[String]("searchid"), x))
      .leftOuterJoin(cvrlog)
      .map {
        x =>
          val u = x._2._1
          var isload = 0
          var iscvr = 0
          if (x._2._2.isDefined) {
            isload = 1
            iscvr = x._2._2.get
          }

          var exptag = u.getAs[String]("exptags").split(",")
            .find(_.startsWith("cvrmodel"))
            .getOrElse("none")
            .replaceFirst("cvrmodel=", "")

          var cvrthres = u.getAs[Int]("cvr_threshold")

          if (cvrthres <= 0) {
            exptag = "none"
            cvrthres = 0
          } else if (cvrthres <= 10000) {
            cvrthres = 1
          } else if (cvrthres <= 40000) {
            cvrthres = 2
          } else if (cvrthres <= 80000) {
            cvrthres = 3
          } else {
            cvrthres = 4
          }

          val mediaid = u.getAs[String]("media_appsid").toInt
          val adslotid = u.getAs[String]("adslotid").toInt
          val slottype = u.getAs[Int]("adslot_type")
          val adclass = u.getAs[Int]("adclass")
          val expcvr = u.getAs[Int]("exp_cvr").toDouble / 1e6
          var isclick = u.getAs[Int]("isclick")
          var spam_click = u.getAs[Int]("spam_click")
          var antispam_score = u.getAs[Int]("antispam_score")
          var realCost = 0
          if (isclick > 0 && antispam_score == 10000) {
            realCost = u.getAs[Int]("price")
          } else {
            realCost = 0
          }
          val cost = realCost

          val k = (mediaid, adslotid, adclass, exptag, cvrthres)
          (k, (iscvr, expcvr, isload, 1, cost, slottype))
      }
      .reduceByKey {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6)
      }
      .filter(_._2._3 > 0)
      .map {
        x =>
          val k = x._1
          val v = x._2
          val d = "%s %s:00:00".format(date, hour)
          val cvr = v._1.toDouble / v._4.toDouble
          val ecvr = v._2 / v._4.toDouble
          val load = v._3.toDouble / v._4.toDouble

          (k._1, k._2, v._6, k._3, k._4, k._5,
            v._5, v._1, v._2, v._3, v._4, cvr, ecvr, load, d)
      }
      .toDF("media_id", "adslot_id", "adslot_type", "adclass", "exp_tag", "threshold",
        "cash_cost", "cvr_num", "exp_cvr_num", "load_num", "click_num", "cvr", "exp_cvr", "load", "date")

    clearReportHourData("report_cvr_prediction_hourly", "%s %s:00:00".format(date, hour), "0")
    cvrData.write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_cvr_prediction_hourly", mariadbProp)
    println("cvr", cvrData.count())

    unionLog.unpersist()

    ctx.stop()
  }

  def cvrPositive(traces: Seq[Row]): Int = {
    var stay = 0
    var click = 0
    var active = 0
    var mclick = 0
    var zombie = 0
    var disactive = 0
    traces.foreach {
      t =>
        t.getAs[String]("trace_type") match {
          case s if s.startsWith("active") => active += 1

          case "disactive" => disactive += 1

          case "buttonClick" => click += 1

          case "clickMonitor" => mclick += 1

          case "inputFocus" => click += 1

          case "press" => click += 1

          case "zombie" => zombie += 1

          case "stay" =>
            if (t.getAs[Int]("duration") > stay) {
              stay = t.getAs[Int]("duration")
            }

          case _ =>
        }
    }

    if (((stay >= 30 && click > 0) || active > 0) && disactive == 0) {
      1
    } else {
      0
    }
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

  def checkReportHourData(tbl: String): Unit = {
    try {
      val cal = Calendar.getInstance()
      cal.add(Calendar.HOUR, -3)
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      val hour = new SimpleDateFormat("HH").format(cal.getTime)

      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |select count(*) as num from report.%s where `date` = "%s" and `hour` = %d
        """.stripMargin.format(tbl, date, hour.toInt)
      val result = stmt.executeQuery(sql)
      var num = 0
      if (result.next()) {
        num = result.getInt("num")
      }
      if (num == 0) {
        val msg = "report.%s rows=%d [%s/%s]".format(tbl, num, date, hour)
        //TODO
      }
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
