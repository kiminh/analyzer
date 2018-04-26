package com.cpc.spark.ml.antimodel.v1

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.log.parser.{ExtValue, TraceLog, UnionLog}
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
    // v6 1 1 1/1 0.5 1 true
    Logger.getRootLogger.setLevel(Level.WARN)

    val version = args(0)
    val dayBefore = args(1).toInt
    val days = args(2).toInt
    val rate = args(3).split("/").map(_.toInt)
    val ttRate = args(4).toFloat  //train/test rate
    val saveFull = args(5).toInt
    val updateDict = args(6).toBoolean
    val ctx = SparkSession.builder()
      .appName("create antispam svm data code:v1 data:" + version)
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dayBefore)
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      println("get data " + date)
      val sqlunion = s"""
                        |select * from dl_cpc.cpc_union_log where `date` ="%s"  and isclick = 1 and adslotid > 0
               """.stripMargin.format(date)
      println("sqlunion:" + sqlunion)

      val union = ctx.sql(sqlunion).as[UnionLog].rdd
      val unionLog = union.randomSplit(Array(ttRate, 1 - ttRate), seed = new Date().getTime)

      if (updateDict) {
        FeatureDict.updateDict(unionLog(0).cache())
      }
      FeatureDict.loadData()
      FeatureDict.saveLua()
      FeatureDict.updateAntispamServerData(ConfigFactory.load())

      val bdict = ctx.sparkContext.broadcast(FeatureDict.dict)
      val train = unionLog(0).filter {
        unionLog =>
          var ret = false
          val label = unionLog.ext.getOrElse("antispam",ExtValue()).int_value
          if ( label > 0 || Random.nextInt(rate(1)) < rate(0)) {
            ret = true
          }
          ret
      }.mapPartitions {
        p =>
          p.map {
            x =>
              val dict = bdict.value
              FeatureParser.parseUnionLog(x, dict)
          }
      }
      train.take(1).foreach(println)
      train.toDF()
        .write
        .mode(SaveMode.Overwrite)
        .text("/user/cpc/antispam/v1/svm/train/" + date)
      println("done", train.filter(_.startsWith("1")).count(), train.count())
      if (saveFull > 0) {
        println("save full data")
        unionLog(1)
          .mapPartitions {
            p =>
              p.map {
                x =>
                  val dict = bdict.value
                  FeatureParser.parseUnionLog(x, dict)
              }
          }
          .toDF()
          .write
          .mode(SaveMode.Overwrite)
          .text("/user/cpc/antispam/v1/svm/test/" + date)
        println("done", unionLog(1).count())
      }
      train.unpersist()
      unionLog(0).unpersist()
     cal.add(Calendar.DATE, -1)
    }
    ctx.stop()
  }
//load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120,
 // trace.client_h,trace.client_w,trace.client_x, trace.client_y, trace.page_x, trace.page_y, trace.scroll_top
  case class TraceLog2(
                       load: Int = 0,
                       active: Int = 0,
                       buttonClick: Int = 0,
                       press: Int = 0,
                       stay1: Int = 0,
                       stay5: Int = 0,
                       stay10: Int = 0,
                       stay30: Int = 0,
                       stay60: Int = 0,
                       stay120: Int = 0,
                       client_h: Int = 0,
                       client_w: Int = 0,
                       client_x: Int = 0,
                       client_y: Int = 0,
                       page_x: Int = 0,
                       page_y: Int = 0,
                       scroll_top: Int = 0,
                       screen_w: Int = 0,
                       screen_h: Int = 0
                 )
  def getDates(dayBefore: Int, days: Int): String = {
    val cal = Calendar.getInstance()
    val parts = new Array[String](days)
    cal.add(Calendar.DATE, -dayBefore)
    val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd")
    for (day <- 0 until days) {
      parts(day) = partitionPathFormat.format(cal.getTime)
      cal.add(Calendar.DATE, -1)
    }
    "'" + parts.mkString("','") + "'"
  }
}

