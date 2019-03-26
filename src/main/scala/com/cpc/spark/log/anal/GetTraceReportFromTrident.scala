package com.cpc.spark.log.anal

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.log.report.AdvTraceReport
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.lit

import scala.collection.mutable.ListBuffer

/**
  * @author fym
  * @version created: 2019-03-25 21:27
  * @desc o.GetTraceReportV3.
  */
object GetTraceReportFromTrident {

  var mariadbUrl = ""
  val mariadbProp = new Properties()
  var mariadb_union_test_write_url = ""
  val mariadb_union_test_prop = new Properties()

  def main(args: Array[String]): Unit = {
    val if_test = 1

    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0)
    val hour = args(1)

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.url")
    mariadbProp.put("user", conf.getString("mariadb.user"))
    mariadbProp.put("password", conf.getString("mariadb.password"))
    mariadbProp.put("driver", conf.getString("mariadb.driver"))

    mariadb_union_test_write_url = conf.getString("mariadb.union_test_write.url")
    mariadb_union_test_prop.put("user", conf.getString("mariadb.union_test_write.user"))
    mariadb_union_test_prop.put("password", conf.getString("mariadb.union_test_write.password"))
    mariadb_union_test_prop.put("driver", conf.getString("mariadb.union_test_write.driver"))

    val ctx = SparkSession.builder()
      .appName("[trident] cpc get trace hour report from %s/%s".format(date, hour))
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
    }.reduceByKey((x, y) => x.sum(y))
      .map(x => x._2)
      .filter(x => x.trace_op1.length < 200)


    println("traceData: " + traceData.count())

    writeToTraceReport(ctx, traceData, date, hour, if_test)

    println("-- GetTraceReport_done --")
  }

  def saveTraceReport(ctx: SparkSession, date: String, hour: String): RDD[AdvTraceReport] = {
    val traceReport = ctx.sql(
      s"""
         |select
         |  tr.searchid
         |  , un.userid as user_id
         |  , un.planid as plan_id
         |  , un.unitid as unit_id
         |  , un.ideaid as idea_id
         |  , tr.day as date
         |  , tr.hour
         |  , tr.trace_type as trace_type
         |  , tr.trace_op1 as trace_op1
         |  , tr.duration as duration
         |  , tr.auto
         |from dl_cpc.cpc_basedata_trace_event as tr
         |left join dl_cpc.cpc_basedata_union_events as un
         |  on tr.searchid = un.searchid
         |where tr.day="%s"
         |  and tr.hour=%s
         |  and un.day="%s"
         |  and un.hour=%s
         |  and un.isclick=1
         |  and un.adslot_type<>7
       """
        .stripMargin
        .format(date, hour, date, hour))
      .rdd
      .cache()

    val sql1 =
      s"""
         |select
         |  ideaid
         |  , sum(isshow) as show
         |  , sum(isclick) as click
         |from dl_cpc.cpc_basedata_union_events
         |where day="%s"
         |  and hour=%s
         |group by ideaid
       """
        .stripMargin
        .format(date, hour)

    val unionRdd = ctx.sql(sql1)
      .rdd
      .map( x => {
        (
          x.getAs[Int]("ideaid"),
          (
            x.getAs[Long]("show").toInt,
            x.getAs[Long]("click").toInt
          )
        )
      }
    )

    val traceData = traceReport
      .filter { trace =>
        trace.getAs[Int]("plan_id") > 0 && trace.getAs[String]("trace_type").length < 100 && trace.getAs[String]("trace_type").length > 1 && trace.getAs[String]("trace_type") != "active_third"
    }
      .map { trace =>
        val trace_type = trace.getAs[String]("trace_type")
        var trace_op1 = ""
        if (trace_type == "apkdown" || trace_type == "lpload" || trace_type == "sdk_incite") {
          trace_op1 = trace.getAs[String]("trace_op1")
        }
        ((trace.getAs[String]("searchid"), trace_type, trace_op1, trace.getAs[Int]("duration"), trace.getAs[Int]("auto")), trace)
    }
      .reduceByKey {
      case (x, y) => x //去重
    }
      .map {
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

    val toResult = traceData
      .join(unionRdd)
      .map {
      case (idea_id, ((user_id, plan_id, unit_id, date, hour, trace_type, trace_op1, duration, auto, count), (impression, click))) =>
        AdvTraceReport(user_id, plan_id, unit_id, idea_id, date, hour, trace_type, trace_op1, duration, auto, count, impression, click)
    }

    println("count:" + toResult.count())
    toResult

  }

  def saveTraceReport_Motivate(ctx: SparkSession, date: String, hour: String): RDD[AdvTraceReport] = {
    val sql =
      s"""
         |select
         |  b.userid as user_id
         |  , b.planid as plan_id
         |  , b.unitid as unit_id
         |  , a.ideaid
         |  , a.trace_type
         |  , a.trace_op1
         |  , a.total_num
         |  , 0 as duration
         |  , 0 as auto
         |  , b.day as date
         |  , b.hour
         |  , b.show
         |  , b.click
         |from
         |  (
         |    select
         |      ta.idea_id
         |      , ta.appname
         |      , ta.adslotid
         |      , ta.trace_type
         |      , ta.trace_op1
         |      , sum(if(ta.num > 0, 1, ta.num)) as total_num
         |    from
         |      (
         |        select
         |          opt['appname'] as appname
         |          , trace_type
         |          , trace_op1
         |          , opt['ideaid'] as idea_id
         |          , trace_op3
         |          , opt['adslotid'] as adslotid
         |          , count(1) as num
         |        from dl_cpc.cpc_basedata_trace_event cut1
         |        where day="%s"
         |          and hour=%s
         |          and trace_type='sdk_incite'
         |          and trace_op1 in ('DOWNLOAD_START','DOWNLOAD_FINISH','INSTALL_FINISH','OPEN_APP','INSTALL_HIJACK')
         |        group by opt['appname']
         |           , trace_type
         |           , trace_op1
         |           , opt['ideaid']
         |           , trace_op3
         |           , opt['adslotid']
         |    ) ta
         |    group by ta.idea_id
         |      , ta.appname
         |      , ta.adslotid
         |      , ta.trace_type
         |      , ta.trace_op1
         |  ) a
         |join
         |  (
         |    select
         |      a.userid
         |      , a.planid
         |      , a.unitid
         |      , a.ideaid
         |      , a.day
         |      , a.hour
         |      , b.show
         |      , b.click
         |    from
         |      (
         |        select
         |          userid
         |          , planid
         |          , unitid
         |          , ideaid
         |          , day
         |          , hour
         |        from dl_cpc.cpc_basedata_union_events
         |        where day="%s"
         |          and hour=%s
         |          and isclick=1
         |          and adslot_type=7
         |        group by userid
         |          , planid
         |          , unitid
         |          , ideaid
         |          , day
         |          , hour
         |      ) a
         |    join
         |      (
         |        select
         |          ideaid
         |          , sum(isshow) as show
         |          , sum(isclick) as click
         |        from dl_cpc.cpc_basedata_union_events
         |        where day="%s"
         |          and hour=%s
         |          and adslot_type=7
         |        group by ideaid
         |      ) b
         |    on a.ideaid = b.ideaid
         |  ) b
         |on a.idea_id = b.ideaid
      """.stripMargin.format(date, hour, date, hour, date, hour)

    val toResult = ctx.sql(sql)
      .rdd
      .map(x =>
        AdvTraceReport(x.getAs[Int]("user_id"),
          x.getAs[Int]("plan_id"),
          x.getAs[Int]("unit_id"),
          x.getAs[String]("idea_id").toInt,
          x.getAs[String]("date"),
          x.getAs[String]("hour"),
          x.getAs[String]("trace_type"),
          x.getAs[String]("trace_op1"),
          x.getAs[Int]("duration"),
          x.getAs[Int]("auto"),
          x.getAs[Long]("total_num").toInt,
          x.getAs[Long]("show").toInt,
          x.getAs[Long]("click").toInt
        )
      )

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

    val cal = Calendar.getInstance()
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    cal.set(date.substring(0, 4).toInt, date.substring(5, 7).toInt - 1, date.substring(8, 10).toInt, hour.toInt, 0, 0)
    cal.add(Calendar.HOUR, -1)
    val fDate = dateFormat.format(cal.getTime)
    val before1hour = fDate.substring(11, 13)

    /*
    trace_log(1h) join api_callback_union_log(3d)
     */
    val sql =
      s"""
         |select
         |  tr.searchid
         |  , un.userid as user_id
         |  , un.planid as plan_id
         |  , un.unitid as unit_id
         |  , un.ideaid as idea_id
         |  , tr.trace_type as trace_type
         |  , tr.trace_op1 as trace_op1
         |  , tr.duration as duration
         |  , tr.auto
         |  , un.isshow
         |  , un.isclick
         |from dl_cpc.cpc_basedata_trace_event as tr
         |left join
         |  (
         |    select
         |      searchid
         |      , userid
         |      , planid
         |      , unitid
         |      , ideaid
         |      , adslot_type
         |      , isshow
         |      , isclick
         |      , day as date
         |      , hour
         |    from dl_cpc.cpc_basedata_union_events
         |    where %s
         |      and isclick=1
         |      and is_api_callback=1
         |  ) as un
         |on tr.searchid = un.searchid
         |where tr.day="%s"
         |  and tr.hour=%s
         |  and un.isclick=1
         |  and un.adslot_type<>7
       """.stripMargin.format(get3DaysBefore(date, hour), date, hour)
    println(sql)

    /*
    没有api回传标记，直接上报到trace
    trace_log(1h) join union_log(2h, 除去api回传is_api_callback=0)
     */
    val sql2 =
      s"""
         |select
         |  tr.searchid
         |  , un.userid as user_id
         |  , un.planid as plan_id
         |  , un.unitid as unit_id
         |  , un.ideaid as idea_id
         |  , tr.trace_type as trace_type
         |  , tr.trace_op1 as trace_op1
         |  , tr.duration as duration
         |  , tr.auto
         |  , un.isshow
         |  , un.isclick
         |from dl_cpc.cpc_basedata_trace_event as tr
         |left join
         |  (
         |    select
         |      a.searchid
         |      , a.userid
         |      , a.planid
         |      , a.unitid
         |      , a.ideaid
         |      , a.isshow
         |      , a.isclick
         |    from dl_cpc.cpc_basedata_union_events a
         |    where a.day="%s"
         |      and a.hour>=%s
         |      and a.hour<=%s
         |      and a.is_api_callback=0
         |      and a.adslot_type<>7
         |      and a.isclick=1
         |  ) as un
         |on tr.searchid = un.searchid
         |where tr.day="%s"
         |  and tr.hour=%s
       """.stripMargin.format(date, before1hour, hour, date, hour)
    println(sql2)

    /*
    应用商城api回传
    trace_log(1h) join motivation_log(3d, trace_type=active_third)
    */
    val sql_moti =
      s"""
         |select
         |  tr.searchid
         |  , un.userid as user_id
         |  , un.planid as plan_id
         |  , un.unitid as unit_id
         |  , un.ideaid as idea_id
         |  , tr.trace_type as trace_type
         |  , tr.trace_op1 as trace_op1
         |  , tr.duration as duration
         |  , tr.auto
         |  , un.isshow
         |  , un.isclick
         |from
         |  (
         |    select
         |      searchid
         |      , opt['ideaid'] as ideaid
         |      , trace_type
         |      , trace_op1
         |      , duration
         |      , auto
         |    from dl_cpc.cpc_basedata_trace_event
         |    where day="%s"
         |      and hour=%s
         |   ) as tr
         |join
         |  (
         |    select
         |      searchid
         |      , userid
         |      , planid
         |      , unitid
         |      , ideaid
         |      , isshow
         |      , isclick
         |      , day as date
         |      , hour
         |      from dl_cpc.cpc_basedata_union_events
         |      where %s
         |        and isclick=1
         |        and adslot_type=7
         |   ) as un
         |on tr.searchid = un.searchid
         |  and tr.ideaid = un.ideaid
       """.stripMargin.format(date, hour, get3DaysBefore(date, hour))
    println(sql_moti)

    val traceReport1 = ctx.sql(sql)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .rdd

    val traceReport2 = ctx.sql(sql2)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .rdd

    val traceReport_moti = ctx.sql(sql_moti)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .rdd

    val traceData = traceReport1
      .union(traceReport2)
      .union(traceReport_moti)
      .filter { trace =>
        trace.getAs[Int]("plan_id") > 0 && trace.getAs[String]("trace_type") == "active_third"
    }
      .map {
      trace =>
        val trace_type = trace.getAs[String]("trace_type")

        ((trace.getAs[String]("searchid"), trace.getAs[Int]("idea_id"), trace.getAs[Int]("auto")), trace)
    }.reduceByKey {
      case (x, y) => x //去重
    }.map {x =>
      val trace = x._2
      val trace_op1 = trace.getAs[String]("trace_op1")

      ((trace.getAs[Int]("user_id"),
        trace.getAs[Int]("plan_id"),
        trace.getAs[Int]("unit_id"),
        trace.getAs[Int]("idea_id"),
        trace.getAs[String]("date"),
        trace.getAs[String]("hour"),
        //auto = 1表明强制注入的trace，要区别清楚
        trace.getAs[Int]("auto")), 1)
    }.reduceByKey {
      case (x, y) => (x + y)
    }.map {x =>
      val trace = x._1
      AdvTraceReport(
        user_id = trace._1,
        plan_id = trace._2,
        unit_id = trace._3,
        idea_id = trace._4,
        date = trace._5,
        hour = trace._6,
        trace_type = "active_third",
        trace_op1 = "",
        duration = 0,
        auto = trace._7,
        total_num = x._2,
        impression = 0,
        click = 0
      )
    }

    println("count:" + traceData.count())
    traceData
  }

  def writeToTraceReport(
                          ctx: SparkSession,
                          toResult: RDD[AdvTraceReport],
                          date: String,
                          hour: String,
                          if_test: Int
                        ): Unit = {
    if (if_test==1) {
      clearReportHourData2("report_trace", date, hour)

      ctx.createDataFrame(toResult)
        .write
        .mode(SaveMode.Append)
        .jdbc(
          mariadb_union_test_write_url,
          "union_test.report_trace",
          mariadb_union_test_prop
        )
    } else {
      clearReportHourData("report_trace", date, hour)

      ctx.createDataFrame(toResult)
        .write
        .mode(SaveMode.Append)
        .jdbc(
          mariadbUrl,
          "report.report_trace",
          mariadbProp
        )
    }




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

      val dateL = s"(day='$datee' and `hour`='$hourr')"
      dateHourList += dateL
    }

    "(" + dateHourList.mkString(" or ") + ")"
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
      Class.forName(mariadb_union_test_prop.getProperty("driver"));
      val conn = DriverManager.getConnection(
        mariadb_union_test_write_url,
        mariadb_union_test_prop.getProperty("user"),
        mariadb_union_test_prop.getProperty("password"));
      val stmt = conn.createStatement();
      val sql =
        """
          |delete from union_test.%s where `date` = "%s" and `hour` = %d
        """.stripMargin.format(tbl, date, hour.toInt);
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
