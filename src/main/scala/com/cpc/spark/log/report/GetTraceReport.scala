package com.cpc.spark.log.report

import java.util.{Calendar, Properties}

import com.cpc.spark.log.parser.{LogParser, UnionLog}
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by Roy on 2017/4/26.
  */
object GetTraceReport {

  val mariadbUrl = "jdbc:mysql://10.9.180.16:3306/adv"

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
    val table = "cpc_union_trace_log"
    val hourBefore = args(1).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore)
    val date = LogParser.dateFormat.format(cal.getTime)
    val hour = LogParser.hourFormat.format(cal.getTime)

    mariadbProp.put("user", "adv")
    mariadbProp.put("password", "advv587")
    mariadbProp.put("driver", "org.mariadb.jdbc.Driver")

    val ctx = SparkSession.builder()
      .appName("cpc get hour report from %s %s/%s".format(table, date, hour))
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val advTraceReport = ctx.sql(
      s"""
         |select union.uid as user_id,union.planid as plan_id ,union.unitid as unit_id ,
         |union.ideaid as idea_id, trace.date as date,trace.hour as hour ,
         |trace.trace_type as trace_type,trace.duration as duration
         |count(trace.trace_type) as count
         |from dl_cpc.cpc_trace_log as trace left join  dl_cpc.cpc_union_log as union on trace.searchid = union.searchid
         |where  trace.`date` = "%s" and `hour` = "%s" group by searchid, trace.trace_type，trace.duration
       """.stripMargin.format(table, date, hour))
      .as[AdvTraceReport]
      .rdd.cache()
    //write hourly data to mysql

    ctx.createDataFrame(advTraceReport)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_trace", mariadbProp)
    ctx.stop()
  }
}
