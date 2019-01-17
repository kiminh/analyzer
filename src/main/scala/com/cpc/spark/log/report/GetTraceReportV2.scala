package com.cpc.spark.log.report

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by Roy on 2017/4/26.
  */
@deprecated
object GetTraceReportV2 {

  var mariadbUrl  = ""

  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |Usage: GetHourReport <hour_before>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)

    val hourBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -hourBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    println("*******************")
    println("date:" + date)
    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password",conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession.builder()
      .appName("cpc get adslot trace hour report from %s".format(date))
      .enableHiveSupport()
      .getOrCreate()

    val traceReport = ctx.sql(
      s"""
         |select tr.searchid, un.userid as user_id
         |,un.planid as plan_id ,un.unitid as unit_id ,
         |un.ideaid as idea_id, tr.date as date,tr.hour,
         |tr.trace_type as trace_type,tr.duration as duration, tr.auto, un.media_appsid as media_id, un.adslotid as adslot_id
         |from dl_cpc.cpc_union_trace_log as tr left join dl_cpc.cpc_union_log as un on tr.searchid = un.searchid
         |where  tr.`date` = "%s"  and un.`date` = "%s"
       """.stripMargin.format(date, date))
      //      .as[TraceReportLog2]
      .rdd.cache()
    val sql1 = ("select ideaid ,media_appsid,adslotid, hour,sum(isshow) as show, sum(isclick) as click " +
      "from dl_cpc.cpc_union_log where `date` = \"%s\"  group by ideaid ,media_appsid,adslotid,hour").format(date)
    val unionRdd = ctx.sql(sql1).rdd.map{
      x =>
        val ideaid  =  x(0).toString().toInt
        val mediaAppsid =  x(1).toString()
        val adslotid  =  x(2).toString()
        val hour = x(3).toString()
        val show : Int = x(4).toString().toInt
        val click : Int = x(5).toString().toInt
        ((ideaid, mediaAppsid, adslotid,hour),(show, click))
    }

    val traceData = traceReport.filter {
      trace =>
        trace.getAs[Int]("plan_id") > 0 && trace.getAs[String]("trace_type").length < 100 && trace.getAs[String]("trace_type").length > 1
    }.map {
      trace =>
        ((trace.getAs[String]("searchid"), trace.getAs[String]("trace_type"),trace.getAs[Int]("duration"), trace.getAs[Int]("auto"),
          trace.getAs[String]("media_id"),trace.getAs[String]("adslot_id"), trace.getAs[String]("hour")), trace)
    }.reduceByKey {
      case (x, y) => x //去重
    }.map{
      case ((searchid, trace_type, duration, auto, media_id, adslot_id,hour), trace) =>
        ((trace.getAs[Int]("user_id"), trace.getAs[Int]("plan_id"), trace.getAs[Int]("unit_id"), trace.getAs[Int]("idea_id"),
          trace.getAs[String]("date"), trace.getAs[String]("hour"), trace.getAs[String]("trace_type"), trace.getAs[Int]("duration"),
          trace.getAs[Int]("auto"),trace.getAs[String]("media_id"),trace.getAs[String]("adslot_id")), 1)
    }.reduceByKey {
      case (x, y) => (x + y)
    }.map{
      case ((user_id, plan_id, unit_id, idea_id, date, hour, trace_type, duration, auto, media_id, adslot_id), count) =>
        ((idea_id, media_id, adslot_id, hour), (user_id, plan_id, unit_id, date, trace_type, duration, auto, count))
    }
    val toResult = traceData.join(unionRdd).map {
      case   ((idea_id, media_id, adslot_id, hour), ((user_id, plan_id, unit_id, date, trace_type, duration, auto, count),(impression, click))) =>
        AdvTraceReport2(user_id, plan_id, unit_id, idea_id, date, hour, trace_type, duration, auto , count, impression, click,media_id,adslot_id)
    }


    println("count:" + toResult.count())
    clearReportHourData("report_adslot_trace", date)
    ctx.createDataFrame(toResult)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_adslot_trace", mariadbProp)
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

  case class TraceReportLog2(
                              searchid:String = "",
                              user_id: Int = 0,
                              plan_id: Int = 0,
                              unit_id: Int = 0,
                              idea_id: Int = 0,
                              date: String = "",
                              hour: String = "",
                              trace_type: String = "",
                              duration: Int = 0,
                              auto: Int = 0,
                              media_id: String = "0",
                              adslot_id: String = "0"
                            )
  case class AdvTraceReport2(
                              user_id: Int = 0,
                              plan_id: Int = 0,
                              unit_id: Int = 0,
                              idea_id: Int = 0,
                              date: String = "",
                              hour: String = "",
                              trace_type: String = "",
                              duration: Int = 0,
                              auto: Int = 0,
                              total_num:Int =0,
                              impression:Int =0,
                              click:Int =0,
                              media_id: String = "0",
                              adslot_id: String = "0"
                            )

}
