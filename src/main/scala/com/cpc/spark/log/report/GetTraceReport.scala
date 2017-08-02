package com.cpc.spark.log.report

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.TraceReportLog
import com.cpc.spark.log.report.GetHourReport.{clearReportHourData, mariadbProp, mariadbUrl}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by Roy on 2017/4/26.
  */
object GetTraceReport {

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
   /* val hourBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)*/
    val date = args(0)
    val hour = args(1)
    println("*******************")
    println("date:" + date)
    println("hour:" + hour)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password",conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    val ctx = SparkSession.builder()
      .appName("cpc get trace hour report from %s/%s".format(date, hour))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val traceReport = ctx.sql(
      s"""
         |select tr.searchid, un.userid as user_id
         |,un.planid as plan_id ,un.unitid as unit_id ,
         |un.ideaid as idea_id, tr.date as date,tr.hour,
         |tr.trace_type as trace_type,tr.duration as duration, tr.auto
         |from dl_cpc.cpc_union_trace_log as tr left join dl_cpc.cpc_union_log as un on tr.searchid = un.searchid
         |where  tr.`date` = "%s" and tr.`hour` = "%s"  and un.`date` = "%s" and un.`hour` = "%s"
       """.stripMargin.format(date, hour, date, hour))
      .as[TraceReportLog]
      .rdd.cache()

    val traceData = traceReport.filter {
      trace =>
        trace.plan_id > 0 && trace.trace_type.length < 100 && trace.trace_type.length > 1
    }.map {
      trace =>
        ((trace.searchid, trace.trace_type,trace.duration, trace.auto), trace)
    }.reduceByKey {
      case (x, y) => x //去重
    }.map{
      case ((searchid, trace_type, duration, auto), trace) =>
        ((trace.user_id, trace.plan_id, trace.unit_id, trace.idea_id, trace.date, trace.hour, trace.trace_type, trace.duration, trace.auto), 1)
    }.reduceByKey {
      case (x, y) => (x + y)
    }.map {
      case ((user_id, plan_id, unit_id, idea_id, date, hour, trace_type, duration, auto), count) =>
        AdvTraceReport(user_id, plan_id, unit_id, idea_id, date, hour, trace_type, duration, auto , count)
    }
    println("*********traceDatatraceDatatraceData**********")
    traceData.collect().foreach(println)
    clearReportHourData("report_trace", date, hour)
    ctx.createDataFrame(traceData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_trace", mariadbProp)
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
