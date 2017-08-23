package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.{ExtValue, TraceLog, UnionLog}
import com.cpc.spark.ml.cvrmodel.v1.FeatureParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}


/*
样本
 */
object CheckCvrLog {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: create cvr svm <daybefore:int> <days:int> <hour:string>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val days = args(1).toInt
    val hour = args(2).trim
    val unitid = args(3).toInt
    val ctx = SparkSession.builder()
      .appName("check cvr results")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      println("get data " + date)

      var hourSql = ""
      if (hour.length > 0) {
        hourSql = "and `hour` in (\"%s\")".format(hour.split(",").mkString("\",\""))
      }

      val sql = s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" %s and unitid = %d
           |
        """.stripMargin.format(date, hourSql, unitid)

      val clicklog = ctx.sql(sql)
        .as[UnionLog].rdd
        //.filter(x => x.exptags.contains("cvrfilter"))
        .map {
          x =>
            (x.searchid, (x, Seq[TraceLog]()))
        }


      val tracelog = ctx.sql(
        s"""
           |select * from dl_cpc.cpc_union_trace_log where `date` = "%s" %s
        """.stripMargin.format(date, hourSql))
        .as[TraceLog].rdd
        .map {
          x =>
            val u: UnionLog = null
            (x.searchid, (u, Seq(x)))
        }

      sum(clicklog.union(tracelog))

      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }

  def sum(ulog: RDD[(String, (UnionLog, Seq[TraceLog]))]): Unit = {
    val sum = ulog
      .reduceByKey {
        (x, y) =>
          var u: UnionLog = null
          if (x._1 != null) {
            u = x._1
          }
          if (y._1 != null) {
            u = y._1
          }
          (u, x._2 ++ y._2)
      }
      .map(_._2)
      .filter(x => x._1 != null)
      .map {
        x =>
          val u = x._1
          var stay = 0
          var click = 0
          var active = 0
          var load = 0d
          x._2.foreach {
            t =>
              t.trace_type match {
                case s if s.startsWith("active") => active += 1

                case "load" => load = 1

                case "buttonClick" => click += 1

                //case "clickMonitor" => click += 1

                case "inputFocus" => click += 1

                case "press" => click += 1

                case "stay" =>
                  if (t.duration > stay) {
                    stay = t.duration
                  }

                case _ =>
              }
          }

          var expcvr = 0d
          if (u.ext != null && load == 1) {
            expcvr = u.ext.getOrElse("exp_cvr", ExtValue()).int_value
          }
          val expctr = u.ext.getOrElse("exp_ctr", ExtValue()).int_value

          var cvr = 0d
          if ((stay >= 30 && click > 0) || active > 0) {
            cvr = 1d
          }

          LogSum(
            adclass = u.ext.getOrElse("media_class", ExtValue()).int_value,
            request = 1,
            show = u.isshow,
            click = u.isclick,
            cvr = cvr,
            load = load,
            cost = u.realCost(),
            expctr = expctr / 1e6,
            expcvr = expcvr / 1e6
          )
      }
      /*
      .map(x => (x.adclass, x))
      .reduceByKey((x, y) => x.sum(y))
      .map(_._2)
      .filter(_.show > 0)
      .toLocalIterator
      .toSeq
      .sortWith(_.show > _.show)
      .foreach {
        sum =>
          println("adclass %s".format(sum.adclass))
          println("show:%.0f click:%.0f fill:%.3f ctr:%.3f ltr:%.3f cvr:%.3f ecvr:%.3f cpm:%.6f cpr:%.6f".format(
            sum.show, sum.click,
            sum.show / sum.request,
            sum.click / sum.show,
            sum.load / sum.click,
            sum.cvr / sum.load,
            sum.expcvr /sum.load,
            sum.cost / sum.show * 1000,
            sum.cost / sum.request * 1000
          ))

      }
      */
      .reduce((x, y) => x.sum(y))

    println("req:%.0f show:%.0f click:%.0f fill:%.3f ctr:%.3f ltr:%.3f cvr:%.3f ecvr:%.3f cpm:%.6f cpr:%.6f".format(
      sum.request, sum.show, sum.click,
      sum.show / sum.request,
      sum.click / sum.show,
      sum.load / sum.click,
      sum.cvr / sum.load,
      sum.expcvr /sum.load,
      sum.cost / sum.show * 10,
      sum.cost / sum.request * 10
    ))
  }

  case class LogSum(
                   adclass: Int = 0,
                   request: Double = 0,
                   show: Double = 0,
                   click: Double = 0,
                   load: Double = 0,
                   cvr: Double = 0,
                   cost: Double = 0,
                   expctr: Double = 0,
                   expcvr: Double = 0
                   ) {

    def sum(s: LogSum): LogSum = {
      copy(
        request = request + s.request,
        show = show + s.show,
        click = click + s.click,
        load = load + s.load,
        cvr = cvr + s.cvr,
        cost = cost + s.cost,
        expctr = expctr + s.expctr,
        expcvr = expcvr + s.expcvr
      )
    }
  }
}

