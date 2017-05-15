package com.cpc.spark.log.report

import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.{LogParser, TraceReportLog}
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by Roy on 2017/4/26.
  */
object GetTraceReport {

  val mariadbUrl = "jdbc:mysql://10.9.180.16:3306/adv"

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
    val hourBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore)
    val date = LogParser.dateFormat.format(cal.getTime)
    val hour = LogParser.hourFormat.format(cal.getTime)
    println("*******************")
    println("date:" + date)
    println("hour:" + hour)

    mariadbProp.put("user", "adv")
    mariadbProp.put("password", "advv587")
    mariadbProp.put("driver", "org.mariadb.jdbc.Driver")

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
         |tr.trace_type as trace_type,tr.duration as duration
         |from dl_cpc.cpc_union_trace_log as tr left join dl_cpc.cpc_union_log as un on tr.searchid = un.searchid
         |where  tr.`date` = "%s" and tr.`hour` = "%s"  and un.`date` = "%s" and un.`hour` = "%s"
       """.stripMargin.format(date, hour, date, hour))
      .as[TraceReportLog]
      .rdd.cache()

    val traceData = traceReport.filter {
      trace =>
        trace.plan_id > 0 && trace.trace_type.length < 100
    }.map {
      trace =>
        ((trace.searchid, trace.trace_type,trace.duration), trace)
    }.reduceByKey {
      case (x, y) => x //去重
    }.map{
      case ((searchid, trace_type, duration), trace) =>
        ((trace.user_id, trace.plan_id, trace.unit_id, trace.idea_id, trace.date, trace.hour, trace.trace_type, trace.duration), 1)
    }.reduceByKey {
      case (x, y) => (x + y)
    }.map {
      case ((user_id, plan_id, unit_id, idea_id, date, hour, trace_type, duration), count) =>
        AdvTraceReport(user_id, plan_id, unit_id, idea_id, date, hour, trace_type, duration, count)
    }
    println("*********traceDatatraceDatatraceData**********")
    traceData.collect().foreach(println)
    ctx.createDataFrame(traceData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "adv.report_trace", mariadbProp)
    ctx.stop()
  }
}
