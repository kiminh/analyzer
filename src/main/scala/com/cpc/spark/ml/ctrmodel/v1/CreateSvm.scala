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
    println((0.12354546346 * 1e3).toInt)
    println((0.999).toInt)
    if (args.length < 5) {
      System.err.println(
        s"""
           |Usage: create svm <version:string> <daybefore:int> <days:int> <rate:int> <hour:string>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val version = args(0)
    val dayBefore = args(1).toInt
    val days = args(2).toInt
    val rate = args(3).toInt
    val hour = args(4)
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

      val svm = ctx.sql(
        s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" %s and isfill = 1 and adslotid > 0
        """.stripMargin.format(date, hourSql))
        .as[UnionLog].rdd
        .filter {
          u =>
            var ret = false
            if (u.isclick == 1 || Random.nextInt(rate) == 0) {
              ret = true
            }
            ret
        }
        .randomSplit(Array(0.5, 0.5), seed = new Date().getTime)(0)
        .map{x => FeatureParser.parseUnionLog(x)}
        .cache()

        if (n == 1) {
          svm.take(1).foreach(println)
        }

      svm.toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/svmdata/" + version + "/" + date)

      println("done")
      svm.unpersist()
      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }
}

