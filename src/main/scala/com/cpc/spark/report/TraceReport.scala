package com.cpc.spark.report

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object TraceReport {
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

    val date = args(0)
    val hour = args(1)

    val ctx = SparkSession.builder()
      .appName("cpc get trace hour report from %s/%s".format(date, hour))
      .enableHiveSupport()
      .getOrCreate()

    val traceReport_Motivate = saveTraceReport_Motivate(ctx, date, hour)
    traceReport_Motivate.repartition(10)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable("test.zhy_trace_motivate")
  }

  def saveTraceReport_Motivate(ctx: SparkSession, date: String, hour: String) = {
    val cal = Calendar.getInstance()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0, 0)
    cal.add(Calendar.HOUR, -1)
    val fDate = dateFormat.format(cal.getTime)
    val before1hour = fDate.substring(11, 13)

    val sql =
      s"""
         |select
         |  a.searchid
         |  ,a.ideaid
         |  ,a.unitid
         |  ,a.planid
         |  ,a.userid
         |  ,a.isshow
         |  ,a.isclick
         |  ,a.day as date
         |  ,a.hour
         |  ,b.appname
         |  ,b.adslotid
         |  ,b.trace_type
         |  ,b.trace_op1
         |  ,b.trace_op3
         |  ,0 as duration
         |  ,0 as auto
         |from
         |  dl_cpc.cpc_basedata_union_events a
         |  join
         |  (
         |    select
         |       searchid
         |      ,opt['ideaid'] as ideaid
         |      ,opt["appname"] as appname
         |      ,opt['adslotid'] as adslotid
         |      ,trace_type
         |      ,trace_op1
         |      ,trace_op3
         |    from dl_cpc.cpc_basedata_trace_event
         |    where day = "$date" and hour = "$hour"
         |      and trace_type = 'sdk_incite'
         |      and trace_op1 in ('DOWNLOAD_START','DOWNLOAD_FINISH','INSTALL_FINISH','OPEN_APP','INSTALL_HIJACK')
         |    group by
         |      searchid
         |      ,opt['ideaid']
         |      ,opt["appname"]
         |      ,opt['adslotid']
         |      ,trace_type
         |      ,trace_op1
         |      ,trace_op3
         |  ) b
         |  on a.searchid=b.searchid and a.ideaid=b.ideaid
         |where a.day = "$date" and a.hour >= "$before1hour" and a.hour <= "$hour" and a.isclick=1 and a.adslot_type=7
   """.stripMargin

    val result = ctx.sql(sql)
      .groupBy(
        col("userid"),
        col("planid"),
        col("unitid"),
        col("ideaid"),
        col("date"),
        col("hour"),
        col("trace_type"),
        col("trace_op1"),
        col("duration"),
        col("auto")
      )
      .agg(
        expr("count(distinct appname,adslotid,trace_op3)").alias("total_num"),
        expr("sum(if(isshow is null,0,isshow))").alias("show"),
        expr("sum(if(isclick is null,0,isclick))").alias("isclick")
      )
    println("count: " + result.count())

    result
  }
}

