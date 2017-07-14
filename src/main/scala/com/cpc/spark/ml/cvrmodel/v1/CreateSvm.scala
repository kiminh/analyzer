package com.cpc.spark.ml.cvrmodel.v1

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.{TraceLog, UnionLog}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

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
}

