package com.cpc.spark.ml.cvrmodel.v4

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.{TraceLog, UnionLog}
import com.cpc.spark.ml.common.FeatureDict
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}


/*
样本
 */
object CreateSvm {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: create cvr svm <version:string> <daybefore:int> <days:int> <hour:string> <adclass:int>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val version = args(0)
    val dayBefore = args(1).toInt
    val days = args(2).toInt
    val hour = args(3)
    val adclass = 0 //args(4).toInt
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
      var adclassSql = ""
      if (adclass > 0) {
        adclassSql = "and (ext['adclass'].int_value = %d or ext['media_class'].int_value = %d)".format(adclass, adclass)
      }
      val sqlStmt =
        s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" %s %s and isclick > 0
           |and media_appsid in ("80000001", "80000002") and adslot_type in (1, 2)
           |and ext['antispam'].int_value = 0
        """.stripMargin.format(date, hourSql, adclassSql)
      println(sqlStmt)
      val clicklog = ctx.sql(sqlStmt)
        .as[UnionLog].rdd
        .map {
          x =>
            (x.searchid, (x, Seq[TraceLog]()))
        }
      println("click log", clicklog.count())

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

      FeatureDict.loadData()
      val bdict = ctx.sparkContext.broadcast(FeatureDict.dict)


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
        .map{
          x =>
            val dict = bdict.value
            FeatureParser.parseUnionLog(dict, x._1, x._2:_*)
        }
        .cache()


      println(svm.take(1).head)
      svm.toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/cvr_" + adclass + "/" + version + "/" + date)

      val n = svm.filter(_.startsWith("1")).count()
      val all = svm.count()
      println("done", n, all - n, all)
      svm.unpersist()
      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }

}

