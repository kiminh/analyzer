package com.cpc.spark.ml.ctrmodel.v1

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.UnionLog
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

/*
样本
 */
object CreateSvm {

  def main(args: Array[String]): Unit = {
    if (args.length < 7) {
      System.err.println(
        s"""
           |Usage: create svm <version:string> <daybefore:int> <days:int>
           | <rate:int> <ttRate:float> <saveFull:int>
           | <hour:string>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val version = args(0)
    val dayBefore = args(1).toInt
    val days = args(2).toInt
    val rate = args(3).toInt
    val ttRate = args(4).toFloat
    val saveFull = args(5).toInt
    val hour = args(6)
    val ctx = SparkSession.builder()
      .appName("create svm data code:v1 data:" + version)
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

      val ulog = ctx.sql(
        s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" %s and isfill = 1 and adslotid > 0
        """.stripMargin.format(date, hourSql))
        .as[UnionLog].rdd
        .randomSplit(Array(ttRate, 1 - ttRate), seed = new Date().getTime)

      val train = ulog(0).filter {
          u =>
            var ret = false
            if (u.isclick == 1 || Random.nextInt(rate) == 0) {
              ret = true
            }
            ret
        }
        .map{x => FeatureParser.parseUnionLog(x)}
        .cache()

      if (n == 1) {
        train.take(1).foreach(println)
      }

      train.toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/svmdata/" + version + "/" + date)
      println("done", train.count())

      if (saveFull > 0) {
        println("save full data")
        ulog(1).map{x => FeatureParser.parseUnionLog(x)}
          .toDF()
          .write
          .mode(SaveMode.Overwrite)
          .text("/user/cpc/svmdata/" + version + "_full/" + date)
        println("done", ulog(1).count())
      }

      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }
}

