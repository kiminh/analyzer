package com.cpc.spark.log.anal

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
object AnalTraceLog {

  var srcRoot = "/gobblin/source/cpc"

  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |usage: analunionlog <hdfs_input> <hdfs_ouput> <hour_before>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    srcRoot = args(0)
    val table = args(1)
    val traceTbl = args(2)
    val hourBefore = args(3).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)
    val spark = SparkSession.builder()
      .appName("cpc anal union log %s partition = %s".format(table, partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val searchData = prepareSource(spark, "cpc_search", hourBefore, 2)
    if (searchData == null) {
      System.err.println("search data is empty")
      System.exit(1)
    }
    var unionData = searchData.map(x => LogParser.parseSearchLog(x.getString(0)))

    val showData = prepareSource(spark, "cpc_show", hourBefore, 2)
    if (showData != null) {
      unionData = unionData.union(showData.map(x => LogParser.parseShowLog(x.getString(0))))
    }

    val clickData = prepareSource(spark, "cpc_click", hourBefore, 2)
    if (clickData != null) {
      unionData = unionData.union(clickData.map(x => LogParser.parseClickLog(x.getString(0))))
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
      }
      .filter(x => x != null && x.date == date && x.hour == hour)
      .cache()

    //write union log data
    spark.createDataFrame(unionData)
      .write
      .mode(SaveMode.Append)
      .format("parquet")
      .partitionBy("date", "hour")
      .saveAsTable("dl_cpc." + table)
    var lastTraceCount = 0L
    var middelTraceCount = 0L
    var traceCount = 0L
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
    val traceData3 = prepareSource(spark, "cpc_trace", hourBefore - 2, 1)
    if (traceData3 != null) {
      val traceData3Rdd = prepareTraceSource(traceData3)
      traceCount += traceData3Rdd.count()
      traceData = traceData.union(traceData3Rdd)
    }
    val traceData4 = prepareSource(spark, "cpc_trace", hourBefore - 3, 1)
    if (traceData4 != null) {
      val traceData4Rdd = prepareTraceSource(traceData4)
      traceCount += traceData4Rdd.count()
      traceData = traceData.union(traceData4Rdd)

    }
    if (traceData1 != null || traceData2 != null|| traceData3 != null || traceData4 != null) {
      val traceRdd1 = traceData
        .reduceByKey {//(x.searchid, (x, Seq[TraceLog]()))
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
      middelTraceCount = traceRdd1.flatMap { x => x._2._2}.count()
       val traceRdd = traceRdd1.flatMap {
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
      traceRdd.toDF()
        .write
        .mode(SaveMode.Overwrite)
        .parquet("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(traceTbl , date, hour))

      lastTraceCount = traceRdd.count()
      println("traceCount：", traceCount)
      println("middelTraceCount：", middelTraceCount)
      println("lastTraceCount：", lastTraceCount)
      traceData.unpersist()
    }
    println("union", unionData.count())
    unionData.unpersist()
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
      .filter(x => x != null && x.searchid.length > 0)
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


