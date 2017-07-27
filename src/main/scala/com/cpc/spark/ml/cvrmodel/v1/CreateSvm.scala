package com.cpc.spark.ml.cvrmodel.v1

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.{TraceLog, UnionLog}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.reflect.internal.util.TableDef.Column
import scala.util.Random

/*
样本
 */
object CreateSvm {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: create cvr svm <version:string> <daybefore:int> <days:int> <hour:string>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val version = args(0)
    val dayBefore = args(1).toInt
    val days = args(2).toInt
    val hour = args(3)
    val ctx = SparkSession.builder()
      .appName("create cvr svm data code:v1 data:" + version)
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

      val clicklog = ctx.sql(
        s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" %s and isclick > 0
           |and media_appsid in ("80000001", "80000002")
        """.stripMargin.format(date, hourSql))
        .as[UnionLog].rdd
        .map {
          x =>
            (x.searchid, (x, Seq[TLog]()))
        }

      val tracelog = ctx.sql(
        s"""
           |select * from dl_cpc.cpc_union_trace_log where `date` = "%s" %s
        """.stripMargin.format(date, hourSql))
        .as[TLog].rdd
        .map {
          x =>
            val u: UnionLog = null
            (x.searchid, (u, Seq(x)))
        }


      /*
      cvr正例条件:
        1 active
        2 (click or press) and stay > 30 and zombie == 0
       */
      val svm = clicklog.union(tracelog)
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
        .map(x => FeatureParser.parseUnionLog(x._1, x._2:_*))
        .cache()

      println(svm.take(1).head)
      svm.toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/cvr_svm/" + version + "/" + date)

      svm.take(1).foreach(println)
      println("done", svm.count())
      svm.unpersist()
      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }

  case class TLog(
                       searchid: String = "",
                       search_timestamp: Int = 0,
                       trace_type: String = "",
                       trace_os: String = "",
                       trace_refer: String = "",
                       trace_version: String = "",
                       trace_click_count: Int = 0,
                       device_orientation: Int = 0,
                       client_w: Float = 0,
                       client_h: Float = 0,
                       screen_w: Float = 0,
                       screen_h: Float = 0,
                       client_x: Float = 0,
                       client_y: Float = 0,
                       page_x: Float = 0,
                       page_y: Float = 0,
                       trace_ttl: Int = 0,
                       scroll_top: Float = 0,
                       trace_op1: String = "",
                       trace_op2: String = "",
                       trace_op3: String = "",
                       duration: Int = 0,
                       date: String = "",
                       hour: String = ""
                     ) {

  }
}

