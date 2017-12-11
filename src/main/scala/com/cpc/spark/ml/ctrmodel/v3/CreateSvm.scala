package com.cpc.spark.ml.ctrmodel.v3

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
    if (args.length < 10) {
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
    val version = args(0)           // v6
    val dayBefore = args(1).toInt   // 1
    val days = args(2).toInt        // 1
    val rate = args(3).split("/").map(_.toInt)  // 85/1000
    val ttRate = args(4).toFloat    // 1
    val saveFull = args(5).toInt    // 1
    val hour = args(6)  // ""
    val mediaInfo= args(7) // 1:qtt; 2:external media; 3:all media
    val adslotInfo = args(8)  // 1:list page; 2:detail page; 3:interactive page
    val info = args(9)

    val mediaAppsidInfo = Map[Int,String](1 -> " and media_appsid in (\"80000001\", \"80000002\")",
                                          2 -> " and media_appsid not in (\"80000001\", \"80000002\")",
                                          3 -> "")

    val adslotTypeInfo = Map[Int,String](1 -> " and adslot_type = 1",
                                         2 -> " and adslot_type = 2",
                                         3 -> " and adslot_type = 3")

    val ctx = SparkSession.builder()
      .appName("create " + info + " svm data code:v6 data:" + version)
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
           |%s %s
           |and ext['antispam'].int_value = 0
        """.stripMargin.format(date,hourSql,
                               mediaAppsidInfo.getOrElse(mediaInfo.toInt,""),
                               adslotTypeInfo.getOrElse(adslotInfo.toInt,"")))
        .as[UnionLog].rdd
        .randomSplit(Array(ttRate, 1 - ttRate), seed = new Date().getTime)


      FeatureDict.loadData()
      //FeatureDict.saveLua()
      FeatureDict.updateServerData(ConfigFactory.load())
      val bdict = ctx.sparkContext.broadcast(FeatureDict.dict)
      val train = ulog(0).filter {
          u =>
            var ret = false
            if (u.isclick == 1 || Random.nextInt(rate(1)) < rate(0)) {
              ret = true
            }
            ret
        }
        .mapPartitions {
          p =>
            val dict = bdict.value
            p.map {
              x =>
                FeatureParser.parseUnionLog(x, dict)
            }
        }
        .cache()

      train.take(1).foreach(println)
      train.toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/svmdata/" + version + "/" + date)
      println("done", train.filter(_.startsWith("1")).count(), train.count())

      if (saveFull > 0) {
        println("save full data")
        ulog(0).union(ulog(1))
          .mapPartitions {
            p =>
              val dict = bdict.value
              p.map {
                x =>
                  FeatureParser.parseUnionLog(x, dict)
              }
          }
          .toDF()
          .write
          .mode(SaveMode.Overwrite)
          .text("/user/cpc/svmdata/" + version + "_full/" + date)
        println("done", ulog(0).union(ulog(1)).count())
      }

      train.unpersist()
      cal.add(Calendar.DATE, 1)
    }

    ctx.stop()
  }
}


