package com.cpc.spark.log.report

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._

/**
  * Created on ${Date} ${Time}
  */
object GetTraceReportV3 {
  var mariadbUrl = ""

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
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    mariadb_amateur_url = conf.getString("mariadb.amateur_write.url")
    mariadb_amateur_prop.put("user", conf.getString("mariadb.amateur_write.user"))
    mariadb_amateur_prop.put("password", conf.getString("mariadb.amateur_write.password"))
    mariadb_amateur_prop.put("driver", conf.getString("mariadb.amateur_write.driver"))

    val ctx = SparkSession.builder()
      .appName("cpc get trace hour report from %s/%s".format(date, hour))
      .enableHiveSupport()
      .getOrCreate()

    val traceReport = saveTraceReport(ctx, date, hour)
    val traceReport_Motivate = saveTraceReport_Motivate(ctx, date, hour) //应用商城（激励下载）
    val traceReport_ApiCallBack = saveTraceReport_ApiCallBack(ctx, date, hour) //用户api回传


    val traceReport1 = traceReport
      .union(traceReport_Motivate)
      .union(traceReport_ApiCallBack)

    val traceData = traceReport1.map {
      x =>
        (x.key, x)
    }.reduceByKey((x, y) =>x.sum(y))
      .map(x => x._2)


    println("traceData: " + traceData.count())

    /*
    ctx.createDataFrame(traceData)
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/warehouse/test.db/zhy_report_trace/%s/%s".format(date, hour))

    ctx.sql(
      """
        |ALTER TABLE test.zhy_report_trace add if not exists PARTITION(`date` = "%s", `hour` = "%s")
        | LOCATION  '/warehouse/test.db/zhy_report_trace/%s/%s'
      """.stripMargin.format(date, hour, date, hour))
  */

    writeToTraceReport(ctx, traceData, date, hour)


    println("GetTraceReport_done")
  }

  def saveTraceReport(ctx: SparkSession, date: String, hour: String): RDD[AdvTraceReport] = {
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
    val unionRdd = ctx.sql(sql1).rdd.map {
      x =>
        val ideaid: Int = x(0).toString().toInt
        val show: Int = x(1).toString().toInt
        val click: Int = x(2).toString().toInt

        (ideaid, (show, click))
    }

    val traceData = traceReport.filter {
      trace =>
        trace.getAs[Int]("plan_id") > 0 && trace.getAs[String]("trace_type").length < 100 && trace.getAs[String]("trace_type").length > 1 &&
          trace.getAs[String]("trace_type") != "active_third"
    }.map {
      trace =>
        val trace_type = trace.getAs[String]("trace_type")
        var trace_op1 = ""
        if (trace_type == "apkdown" || trace_type == "lpload" || trace_type == "sdk_incite") {
          trace_op1 = trace.getAs[String]("trace_op1")
        }
        ((trace.getAs[String]("searchid"), trace_type, trace_op1, trace.getAs[Int]("duration"), trace.getAs[Int]("auto")), trace)
    }.reduceByKey {
      case (x, y) => x //去重
    }.map {
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
    }.map {
      case ((user_id, plan_id, unit_id, idea_id, date, hour, trace_type, trace_op1, duration, auto), count) =>
        (idea_id, (user_id, plan_id, unit_id, date, hour, trace_type, trace_op1, duration, auto, count))
    }
    val toResult = traceData.join(unionRdd).map {
      case (idea_id, ((user_id, plan_id, unit_id, date, hour, trace_type, trace_op1, duration, auto, count), (impression, click))) =>
        AdvTraceReport(user_id, plan_id, unit_id, idea_id, date, hour, trace_type, trace_op1, duration, auto, count, impression, click)
    }

    println("count:" + toResult.count())
    toResult

  }

  /**
    * 应用商城
    *
    * @param ctx
    * @param date
    * @param hour
    * @return
    */
  def saveTraceReport_Motivate(ctx: SparkSession, date: String, hour: String): RDD[AdvTraceReport] = {
    val traceReport = ctx.sql(
      s"""
         |select tr.searchid
         |      ,un.userid as user_id
         |      ,un.planid as plan_id
         |      ,un.unitid as unit_id
         |      ,un.ideaid as idea_id
         |      ,un.date
         |      ,un.hour
         |      ,tr.trace_type as trace_type
         |      ,tr.trace_op1 as trace_op1
         |      ,tr.duration as duration
         |      ,tr.auto
         |from dl_cpc.logparsed_cpc_trace_minute as tr left join dl_cpc.cpc_motivation_log as un on tr.searchid = un.searchid and tr.opt['ideaid']=un.ideaid
         |where  tr.`thedate` = "%s" and tr.`thehour` = "%s"  and un.`date` = "%s" and un.`hour` = "%s" and un.isclick = 1
       """.stripMargin.format(date, hour, date, hour))
      .rdd

    val sql1 = "select ideaid , sum(isshow) as show, sum(isclick) as click from dl_cpc.cpc_motivation_log where `date` = \"%s\" and `hour` =\"%s\" group by ideaid ".format(date, hour)
    val unionRdd = ctx.sql(sql1).rdd.map {
      x =>
        val ideaid: Int = x(0).toString().toInt
        val show: Int = x(1).toString().toInt
        val click: Int = x(2).toString().toInt

        (ideaid, (show, click))
    }

    val traceData = traceReport.filter {
      trace =>
        trace.getAs[Int]("plan_id") > 0 && trace.getAs[String]("trace_type") == "sdk_incite" && (trace.getAs[String]("trace_op1") == "DOWNLOAD_START" ||
          trace.getAs[String]("trace_op1") == "DOWNLOAD_FINISH" || trace.getAs[String]("trace_op1") == "INSTALL_FINISH" || trace.getAs[String]("trace_op1") == "OPEN_APP" &&
          trace.getAs[String]("trace_op1") == "INSTALL_HIJACK" || trace.getAs[String]("trace_op1") == "INSTALL_ABORT")
    }.map {
      trace =>
        val trace_type = trace.getAs[String]("trace_type")
        var trace_op1 = trace.getAs[String]("trace_op1")

        ((trace.getAs[String]("searchid"), trace.getAs[Int]("idea_id"), trace_type, trace_op1, trace.getAs[Int]("duration"), trace.getAs[Int]("auto")), trace)
    }.reduceByKey {
      case (x, y) => x //去重
    }.map {
      case ((searchid, idea_id, trace_type, trace_op1, duration, auto), trace) =>
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
    }.map {
      case ((user_id, plan_id, unit_id, idea_id, date, hour, trace_type, trace_op1, duration, auto), count) =>
        (idea_id, (user_id, plan_id, unit_id, date, hour, trace_type, trace_op1, duration, auto, count))
    }
    val toResult = traceData.join(unionRdd).map {
      case (idea_id, ((user_id, plan_id, unit_id, date, hour, trace_type, trace_op1, duration, auto, count), (impression, click))) =>
        AdvTraceReport(user_id, plan_id, unit_id, idea_id, date, hour, trace_type, trace_op1, duration, auto, count, impression, click)
    }

    println("motivate count:" + toResult.count())
    toResult
  }

  /**
    * 用户api回传
    *
    * @param ctx
    * @param date
    * @param hour
    */
  def saveTraceReport_ApiCallBack(ctx: SparkSession, date: String, hour: String): RDD[AdvTraceReport] = {

    val sql =
      s"""
         |select tr.searchid
         |      ,un.userid as user_id
         |      ,un.planid as plan_id
         |      ,un.unitid as unit_id
         |      ,un.ideaid as idea_id
         |      ,tr.trace_type as trace_type
         |      ,tr.trace_op1 as trace_op1
         |      ,tr.duration as duration
         |      ,tr.auto
         |from dl_cpc.logparsed_cpc_trace_minute as tr
         |left join
         |(select searchid, userid, planid, unitid, ideaid, adslot_type, isclick, date, hour from dl_cpc.cpc_user_api_callback_union_log where %s) as un on tr.searchid = un.searchid
         |where  tr.`thedate` = "%s" and tr.`thehour` = "%s" and un.isclick = 1 and un.adslot_type <> 7
       """.stripMargin.format(get3DaysBefore(date, hour), date, hour)
    println(sql)

    val traceReport = ctx.sql(sql)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .rdd

    val sql1 =
      """
        |select ideaid , sum(isshow) as show, sum(isclick) as click
        |from dl_cpc.cpc_user_api_callback_union_log
        |where %s group by ideaid
      """.stripMargin.format(get3DaysBefore(date, hour))
    println(sql1)

    val unionRdd = ctx.sql(sql1).rdd.map {
      x =>
        val ideaid: Int = x(0).toString().toInt
        val show: Int = x(1).toString().toInt
        val click: Int = x(2).toString().toInt

        (ideaid, (show, click))
    }

    val traceData = traceReport.filter {
      trace =>
        trace.getAs[Int]("plan_id") > 0 && trace.getAs[String]("trace_type") == "active_third"
    }.map {
      trace =>
        val trace_type = trace.getAs[String]("trace_type")
        val trace_op1 = trace.getAs[String]("trace_op1")

        ((trace.getAs[String]("searchid"), trace_type, trace_op1, trace.getAs[Int]("duration"), trace.getAs[Int]("auto")), trace)
    }.reduceByKey {
      case (x, y) => x //去重
    }.map {
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
    }.map {
      case ((user_id, plan_id, unit_id, idea_id, date, hour, trace_type, trace_op1, duration, auto), count) =>
        (idea_id, (user_id, plan_id, unit_id, date, hour, trace_type, trace_op1, duration, auto, count))
    }
    val toResult = traceData.join(unionRdd).map {
      case (idea_id, ((user_id, plan_id, unit_id, date, hour, trace_type, trace_op1, duration, auto, count), (impression, click))) =>
        AdvTraceReport(user_id, plan_id, unit_id, idea_id, date, hour, trace_type, trace_op1, duration, auto, count, impression, click)
    }

    println("count:" + toResult.count())
    toResult

  }

  def writeToTraceReport(ctx: SparkSession, toResult: RDD[AdvTraceReport], date: String, hour: String): Unit = {
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

  }

  def get3DaysBefore(date: String, hour: String): String = {
    val dateHourList = ListBuffer[String]()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val cal = Calendar.getInstance()
    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0, 0)
    for (t <- 0 to 72) {
      if (t > 0) {
        cal.add(Calendar.HOUR, -1)
      }
      val formatDate = dateFormat.format(cal.getTime)
      val datee = formatDate.substring(0, 10)
      val hourr = formatDate.substring(11, 13)

      val dateL = s"(`date`='$datee' and `hour`='$hourr')"
      dateHourList += dateL
    }

    "(" + dateHourList.mkString(" or ") + ")"
  }

  def clearReportHourData(tbl: String, date: String, hour: String): Unit = {
    try {
      println("~~~~~clearReportHourData~~~~~")
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
