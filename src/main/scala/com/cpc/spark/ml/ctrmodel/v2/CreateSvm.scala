package com.cpc.spark.ml.ctrmodel.v2

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.ml.common.FeatureDict
import com.typesafe.config.ConfigFactory
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
           |Usage: create svm <version:string> <daybefore:int> <days:int>
           | <hour:string> <updatedict>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    val version = args(0)
    val dayBefore = args(1).toInt
    val days = args(2).toInt
    val updateDict = args(3).toBoolean
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
      val ulog = ctx.sql(
        s"""
           |select * from dl_cpc.cpc_union_log where `date` = "%s" %s and isshow = 1
           |and media_appsid in ("80000001", "80000002") and adslot_type in (1, 2)
           |and ext['antispam'].int_value = 0
        """.stripMargin.format(date, hourSql))
        .as[UnionLog].rdd
        .repartition(4000)

      val logNum = ulog.count()
      //最多拿2亿的数据
      val rate = math.min(2e8 / logNum.toDouble, 1)
      println("log num", logNum, rate)
      val sample = ulog.randomSplit(Array(rate, 1 - rate))(0).cache()

      if (updateDict) {
        FeatureDict.updateDict(sample)
      }
      FeatureDict.loadData()
      if (updateDict) {
        FeatureDict.updateServerData(ConfigFactory.load())
      }
      val bdict = ctx.sparkContext.broadcast(FeatureDict.dict)
      val svm = sample
        .mapPartitions {
          p =>
            val dict = bdict.value
            p.map {
              x =>
                FeatureParser.parseUnionLog(x, dict)
            }
        }
        .cache()

      svm.take(1).foreach(println)
      val m = svm.count()
      val pn = svm.filter(_.startsWith("1")).count()
      svm.toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/ctr_svm/" + version + "/" + date)
      println("done", pn, m - pn, m)

      sample.unpersist()
      svm.unpersist()
      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }
}

