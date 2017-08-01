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
           |select * from dl_cpc.cpc_union_log where `date` = "%s" %s
           |and media_appsid in ("80000001", "80000002")
        """.stripMargin.format(date, hourSql)

      val clicklog = ctx.sql(sql)
        .as[UnionLog].rdd.cache()
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
        .cache()

      println("cvr ctr")
      val cvrctrlog = clicklog.filter(x => x.exptags.contains("cvr_v1") && x.exptags.contains("ctrmodel=v1"))
        .map {
          x =>
            (x.searchid, (x, Seq[TraceLog]()))
        }
      val svmctr = sum(cvrctrlog.union(tracelog))
      println("cvr:%.3f predict:%.3f ctr:%.3f sum:%.0f".format(
        svmctr._1 / svmctr._3, svmctr._2 / svmctr._3, svmctr._4 / svmctr._3, svmctr._3))

      println("cvr")
      val cvrlog = clicklog.filter(x => x.exptags.contains("cvr_v1"))
        .map {
          x =>
            (x.searchid, (x, Seq[TraceLog]()))
        }
      val svm = sum(cvrlog.union(tracelog))
      println("cvr:%.3f predict:%.3f sum:%.0f".format(svm._1 / svm._3, svm._2 / svm._3, svm._3))

      println("nocvr")
      val nocvrlog = clicklog.filter(x => !x.exptags.contains("cvr_v1"))
        .map {
          x =>
            (x.searchid, (x, Seq[TraceLog]()))
        }
      val svmall = sum(nocvrlog.union(tracelog))
      println("cvr:%.3f predict:%.3f sum:%.0f".format(svmall._1 / svmall._3, svmall._2 / svmall._3, svmall._3))

      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }

  def sum(ulog: RDD[(String, (UnionLog, Seq[TraceLog]))]): (Double, Double, Double, Double) = {
    ulog
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
      .filter(x => x._1 != null && x._2.nonEmpty)
      .map {
        x =>
          val u = x._1
          val traces = x._2
          var stay = 0
          var click = 0
          var active = 0
          traces.foreach {
            t =>
              t.trace_type match {
                case s if s.startsWith("active") => active += 1

                case "buttonClick" => click += 1

                case "clickMonitor" => click += 1

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
          if (u.ext != null) {
            expcvr = u.ext.getOrElse("exp_cvr", ExtValue()).int_value
          }
          val expctr = u.ext.getOrElse("exp_ctr", ExtValue()).int_value

          var cvr = 0d
          if ((stay >= 30 && click > 0) || active > 0) {
            cvr = 1d
          }

          (cvr, expcvr/1e6, 1d, expctr/1e6)
      }
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4))
  }
}

