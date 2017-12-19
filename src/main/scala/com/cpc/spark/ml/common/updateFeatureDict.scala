package com.cpc.spark.ml.common

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.UnionLog
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
更新FeatureDict文件
 */
object updateFeatureDict {

  def main(args: Array[String]): Unit = {
    if (args.length < 6) {
      System.err.println(
        s"""
           |Usage: create svm <version:string> <daybefore:int> <days:int>
           | <rate:int> <ttRate:float> <saveFull:int>
           | <hour:string> <updatedict>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val version = args(0)
    val dayBefore = args(1).toInt
    val days = args(2).toInt
    val ttRate = args(3).toFloat  //train/test rate
    val updateDict = args(4).toBoolean
    val hour = args(5)

    val ctx = SparkSession.builder()
      .appName("update feature dict:" + version)
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
           |select * from dl_cpc.cpc_union_log where `date` = "%s" %s and isshow = 1
           |and ext['antispam'].int_value = 0
        """.stripMargin.format(date, hourSql))
        .as[UnionLog].rdd
        .randomSplit(Array(ttRate, 1 - ttRate), seed = new Date().getTime)

      if (updateDict) {
        FeatureDict.updateDict(ulog(0).cache())
      }
    }

    ctx.stop()
  }
}

