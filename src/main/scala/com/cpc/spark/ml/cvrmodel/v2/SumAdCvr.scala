package com.cpc.spark.ml.cvrmodel.v2

import java.io.FileWriter
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.{TraceLog, UnionLog}
import com.cpc.spark.ml.common.FeatureDict
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.io.Source


/*
样本
 */
object SumAdCvr {

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: sum cvr <daybefore:int> <days:int>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val dayBefore = args(0).toInt
    val days = args(1).toInt
    val cvrfile = args(2).toBoolean
    val ctx = SparkSession.builder()
      .appName("sum ad cvr")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      println("get data " + date)
      val sqlStmt =
        s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" and isclick > 0
           |and media_appsid in ("80000001", "80000002") and adslot_type in (1, 2)
           |and ext['antispam'].int_value = 0
        """.stripMargin.format(date)
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
           |select * from dl_cpc.cpc_union_trace_log where `date` = "%s"
        """.stripMargin.format(date))
        .as[TraceLog].rdd
        .map {
          x =>
            val u: UnionLog = null
            (x.searchid, (u, Seq(x)))
        }

      val cvrlog = clicklog.union(tracelog)
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
        .cache()

      val adcvrlog = cvrlog.map(x => (x._1.ideaid, (1, if (FeatureParser.cvrPositive(x._2:_*)) 1 else 0)))
        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
        .map(x => (x._1, x._2._1, x._2._2))
        .filter(x => x._1 > 0 && x._3 > 0)

      val w = new FileWriter("/data/cpc/anal/data/adcvr/adcvr_%s.txt".format(date))
      adcvrlog.toLocalIterator
        .foreach {
          x =>
            w.write("%d %d %d\n".format(x._1, x._2, x._3))
        }
      w.close()

      //create cvr file
      if (cvrfile) {
        val cal = Calendar.getInstance()
        val adcvrSum = mutable.Map[Int, (Int, Int)]()
        cal.add(Calendar.DATE, -20)
        for (n <- 0 until 20) {
          val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
          cal.add(Calendar.DATE, 1)
          val adcvrfile = "/data/cpc/anal/data/adcvr/adcvr_%s.txt".format(date)
          val tmp = Source.fromFile(adcvrfile, "UTF-8")
            .getLines()
            .filter(_.length > 0)
            .foreach {
              line =>
                val row = line.split(" ")
                val adid = row(0).toInt
                val click = row(1).toInt
                val cvr = row(2).toInt
                val tmp = adcvrSum.getOrElse(adid, (0, 0))
                adcvrSum.update(adid, (tmp._1 + click, tmp._2 + cvr))
            }
        }

        val w = new FileWriter("/data/cpc/anal/conf/mldict/adcvr.txt")
        adcvrSum
          .foreach {
            case (adid, (click, cvr)) =>
              if (cvr > 1) {
                w.write("%d %.6f\n".format(adid, cvr.toDouble / click.toDouble))
              }
          }
      }

      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }

}

