package com.cpc.spark.log.report

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.{ExtValue, LogParser, TraceLog, UnionLog}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable


/**
  * Created by Roy on 2017/4/18.
  */
object LoseUnionLogStatis {

  var srcRoot = "/gobblin/source/cpc"

  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println(
        s"""
           |usage: analunionlog  <hourBefore>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    srcRoot ="/gobblin/source/cpc"
    val hourBefore = args(0).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)
    val spark = SparkSession.builder()
      .appName("cpc anal union log last statistics hour:%s".format(hour))
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val searchData = prepareSource(spark, "cpc_search", hourBefore, 2)
    if (searchData == null) {
      System.err.println("search data is empty")
      System.exit(1)
    }
    var unionData = searchData.map(x => LogParser.parseSearchLog(x.getString(0)))
    val searchCount= unionData.count()
    var lastSearchCount= 0L
    var lastShowCount = 0L
    var showCount = 0L
    var lastClickCount = 0L
    var clickCount = 0L
    var lastTraceCount = 0L
    var traceCount = 0L
    val showData = prepareSource(spark, "cpc_show", hourBefore, 2)
    if (showData != null) {
      val showDataRdd = showData.map(x => LogParser.parseShowLog(x.getString(0)))
      showCount = showDataRdd.count()
      unionData = unionData.union(showDataRdd)
    }

    val clickData = prepareSource(spark, "cpc_click", hourBefore, 2)
    if (clickData != null) {
      val clickDataRdd = clickData.map(x => LogParser.parseClickLog(x.getString(0)))
      clickCount = clickDataRdd.count()
      unionData = unionData.union(clickDataRdd)
    }
    unionData = unionData
      .filter(x => x != null && x.searchid.length > 0)
      .map(x => (x.searchid, Seq(x)))
      .reduceByKey {
        (x, y) =>
          x ++ y
      }
      .map {
        x =>
          var log = x._2.find(_.timestamp > 0).getOrElse(null)
          if (log != null) {
            x._2.foreach {
              u =>
                if (u.isshow == 1) {
                  log = log.copy(
                    isshow = u.isshow,
                    show_timestamp = u.show_timestamp,
                    show_network = u.show_network,
                    show_ip = u.show_ip
                  )
                }
                if (u.isclick == 1) {
                  if (u.isSpamClick() == 1) {
                    var ext = mutable.Map[String, ExtValue]()
                    if (log.ext != null) {
                      ext = ext ++ log.ext
                    }
                    val spam = ext.getOrElse("spam_click", ExtValue())
                    ext.update("spam_click", ExtValue(int_value = spam.int_value + 1))
                    log = log.copy(
                      ext = ext
                    )
                  } else {
                    log = log.copy(
                      isclick = u.isclick,
                      click_timestamp = u.click_timestamp,
                      antispam_score = u.antispam_score,
                      antispam_rules = u.antispam_rules,
                      click_network = u.click_network,
                      click_ip = u.click_ip
                    )
                  }
                }
            }
          }
          log
      }.filter(x => x != null).cache()
    lastSearchCount = unionData.count()
    lastClickCount = unionData.filter(x => x.isclick >0).count()
    lastShowCount = unionData.filter(x => x.isshow >0).count()

    unionData = unionData.filter(x => x != null && x.date == date && x.hour == hour)
    //write union log data
   /* spark.createDataFrame(unionData)
      .write
      .mode(SaveMode.Append)
      .format("parquet")
      .partitionBy("date", "hour")
      .saveAsTable("dl_cpc." + table)*/

    var traceData = unionData.map(x => (x.searchid, (x, Seq[TraceLog]())))
    val traceData1 = prepareSource(spark, "cpc_trace", hourBefore, 1)
    if (traceData1 != null) {
      val traceData1Rdd = prepareTraceSource(traceData1)
      traceCount += traceData1Rdd.count()
      traceData = traceData.union(traceData1Rdd)
    }
    val traceData2 = prepareSource(spark, "cpc_trace", hourBefore - 1, 1)
    if (traceData2 != null) {
      val traceData2Rdd = prepareTraceSource(traceData2)
      traceCount += traceData2Rdd.count()
      traceData = traceData.union(traceData2Rdd)
    }
    if (traceData1 != null || traceData2 != null) {
      val traceRdd = traceData
        .reduceByKey { //(x.searchid, (x, Seq[TraceLog]()))
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
        .flatMap {
          x =>
            val u = x._2._1
            x._2._2.filter(x => u != null)
              .map {
                t =>
                  t.copy(
                    search_timestamp = u.timestamp,
                    date = u.date,
                    hour = u.hour
                  )
              }
        }.cache()

      lastTraceCount = traceRdd.count()
      traceData.unpersist()
    }
    unionData.unpersist()
    println("searchCount: "+ searchCount + ";lastSearchCount:" + lastSearchCount + ";rate:" + lastSearchCount.toFloat/searchCount.toFloat)
    println("showCount: "+ showCount + ";lastShowCount:" + lastShowCount + ";rate:" + lastShowCount.toFloat/showCount.toFloat)
    println("clickCount: "+ clickCount + ";lastClickCount:" + lastClickCount + ";rate:" + lastClickCount.toFloat/clickCount.toFloat)
    println("traceCount: "+ traceCount + ";lastTraceCount:" + lastTraceCount + ";rate:" + lastTraceCount.toFloat/traceCount.toFloat)
    spark.stop()
  }

  val schema = StructType(Array(
    StructField("log_timestamp", LongType, true),
    StructField("ip", StringType, true),
    StructField("field", MapType(StringType,
      StructType(Array(
        StructField("int_type", IntegerType, true),
        StructField("long_type", LongType, true),
        StructField("float_type", FloatType, true),
        StructField("string_type", StringType, true))), true), true)))

  /*
  cpc_search cpc_show cpc_click cpc_trace cpc_charge
   */
  def prepareSource(ctx: SparkSession, src: String, hourBefore: Int, hours: Int): rdd.RDD[Row] = {
    try {
      val input = "%s/%s/%s".format(srcRoot, src, getDateHourPath(hourBefore, hours))
      println("input:" + input)
      val baseData = ctx.read.schema(schema).parquet(input)
      val tbl = "%s_data_%d".format(src, hourBefore)
      baseData.createTempView(tbl)
      ctx.sql("select field['%s'].string_type from %s".format(src, tbl)).rdd
    } catch {
      case e: Exception => null
    }
  }

  def prepareTraceSource(src: rdd.RDD[Row]): rdd.RDD[(String, (UnionLog, Seq[TraceLog]))] = {
    src.map(x => LogParser.parseTraceLog(x.getString(0)))
      .filter(x => x != null && x.searchid.length > 0 && x.searchid != "none")
      .map {
        x =>
          val u: UnionLog = null
          (x.searchid, (u, Seq(x)))
      }
  }

  def getDateHourPath(hourBefore: Int, hours: Int): String = {
    val cal = Calendar.getInstance()
    val parts = new Array[String](hours)
    cal.add(Calendar.HOUR, -hourBefore)
    for (h <- 0 until hours) {
      parts(h) = partitionPathFormat.format(cal.getTime)
      cal.add(Calendar.HOUR, 1)
    }
    "{" + parts.mkString(",") + "}"
  }
}


