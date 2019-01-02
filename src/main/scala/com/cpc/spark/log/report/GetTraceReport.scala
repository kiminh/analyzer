package com.cpc.spark.log.report

import java.sql.DriverManager
import java.util.Properties

import com.cpc.spark.log.report.GetHourReport.{mariadb_amateur_prop, mariadb_amateur_url}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * Created by Roy on 2017/4/26.
  */
@deprecated
object GetTraceReport {

  var mariadbUrl  = ""

  val mariadbProp = new Properties()

  var mariadb_amateur_url = ""
  val mariadb_amateur_prop = new Properties()

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

    mariadb_amateur_url=conf.getString("mariadb.amateur_write.url")
    mariadb_amateur_prop.put("user",conf.getString("mariadb.amateur_write.user"))
    mariadb_amateur_prop.put("password", conf.getString("mariadb.amateur_write.password"))
    mariadb_amateur_prop.put("driver", conf.getString("mariadb.amateur_write.driver"))

    val ctx = SparkSession.builder()
      .appName("cpc get trace hour report from %s/%s".format(date, hour))
      .enableHiveSupport()
      .getOrCreate()


    val traceReport = ctx.sql(
      s"""
         |select tr.searchid, un.userid as user_id
         |,un.planid as plan_id ,un.unitid as unit_id ,
         |un.ideaid as idea_id, tr.date as date,tr.hour,
         |tr.trace_type as trace_type,tr.trace_op1 as trace_op1 ,tr.duration as duration, tr.auto
         |from dl_cpc.cpc_union_trace_log as tr left join dl_cpc.cpc_union_log as un on tr.searchid = un.searchid
         |where  tr.`date` = "%s" and tr.`hour` = "%s"  and un.`date` = "%s" and un.`hour` = "%s" and un.isclick = 1
       """.stripMargin.format(date, hour, date, hour))
      //      .as[TraceReportLog]
      .rdd.cache()
    val sql1 = "select ideaid , sum(isshow) as show, sum(isclick) as click from dl_cpc.cpc_union_log where `date` = \"%s\" and `hour` =\"%s\" group by ideaid ".format(date, hour)
    val unionRdd = ctx.sql(sql1).rdd.map{
      x =>
        val ideaid : Int =  x(0).toString().toInt
        val show : Int = x(1).toString().toInt
        val click : Int = x(2).toString().toInt

        (ideaid,(show, click))
    }

    val traceData = traceReport.filter {
      trace =>
        trace.getAs[Int]("plan_id") > 0 && trace.getAs[String]("trace_type").length < 100 && trace.getAs[String]("trace_type").length > 1
    }.map {
      trace =>
        val trace_type = trace.getAs[String]("trace_type")
        var trace_op1 = ""
        if(trace_type == "apkdown" || trace_type == "lpload" || trace_type == "sdk_incite"){
          trace_op1 = trace.getAs[String]("trace_op1")
        }
        ((trace.getAs[String]("searchid"), trace_type, trace_op1,trace.getAs[Int]("duration"), trace.getAs[Int]("auto")), trace)
    }.reduceByKey {
      case (x, y) => x //去重
    }.map{
      case ((searchid, trace_type, trace_op1, duration, auto), trace) =>
        ((trace.getAs[Int]("user_id"),
          trace.getAs[Int]("plan_id"),
          trace.getAs[Int]("unit_id"),
          trace.getAs[Int]("idea_id"),
          trace.getAs[String]("date"),
          trace.getAs[String]("hour"),
          trace.getAs[String]("trace_type"),
          trace_op1,
          trace.getAs[Int]("duration"),
          trace.getAs[Int]("auto")), 1)
    }.reduceByKey {
      case (x, y) => (x + y)
    }.map{
      case ((user_id, plan_id, unit_id, idea_id, date, hour, trace_type, trace_op1, duration, auto), count) =>
        (idea_id, (user_id, plan_id, unit_id, date, hour, trace_type,trace_op1, duration, auto, count))
    }
    val toResult = traceData.join(unionRdd).map {
      case   (idea_id, ((user_id, plan_id, unit_id, date, hour, trace_type, trace_op1, duration, auto, count),(impression, click))) =>
        AdvTraceReport(user_id, plan_id, unit_id, idea_id, date, hour, trace_type, trace_op1, duration, auto , count, impression, click)
    }


    println("count:" + toResult.count())
    clearReportHourData("report_trace", date, hour)
    ctx.createDataFrame(toResult)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_trace", mariadbProp)

    clearReportHourData2("report_trace", date, hour)
    ctx.createDataFrame(toResult)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadb_amateur_url, "report.report_trace", mariadb_amateur_prop)

    ctx.stop()
    println("GetTraceReport_done")
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
  def clearReportHourData2(tbl: String, date: String, hour: String): Unit = {
    try {
      Class.forName(mariadb_amateur_prop.getProperty("driver"));
      val conn = DriverManager.getConnection(
        mariadb_amateur_url,
        mariadb_amateur_prop.getProperty("user"),
        mariadb_amateur_prop.getProperty("password"));
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
