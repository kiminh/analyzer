package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.{LogParser, TraceLog, UnionLog}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._


/**
  * Created by Roy on 2017/4/18.
  */
object AnalUnionLog {

  var srcRoot = "/gobblin/source/cpc"

  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: AnalUnionLog <hdfs_input> <hdfs_ouput> <hour_before>
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
    val date = LogParser.dateFormat.format(cal.getTime)
    val hour = LogParser.hourFormat.format(cal.getTime)
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
      .map(x => (x.searchid, x))
      .reduceByKey {
        (x, y) =>
          if (y.timestamp > 0) {
            merge(y, x)
          } else {
            merge(x, y)
          }
      }
      .map(_._2)
      .filter(x => x.date == date && x.hour == hour)
      .cache()

    //write union log data
    spark.createDataFrame(unionData)
      .write
      .mode(SaveMode.Append)
      .format("parquet")
      .partitionBy("date", "hour")
      .saveAsTable("dl_cpc." + table)

    var traceData = unionData.map(x => (x.searchid, (x, Seq[TraceLog]())))
    val traceData1 = prepareSource(spark, "cpc_trace", hourBefore, 1)
    if (traceData1 != null) {
      traceData = traceData.union(prepareTraceSource(traceData1))
    }
    val traceData2 = prepareSource(spark, "cpc_trace", hourBefore - 1, 1)
    if (traceData2 != null) {
      traceData = traceData.union(prepareTraceSource(traceData2))
    }
    if (traceData1 != null || traceData2 != null) {
        traceData.reduceByKey {
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
        }
        .toDF()
        .write
        .mode(SaveMode.Append)
        .format("parquet")
        .partitionBy("date", "hour")
        .saveAsTable("dl_cpc." + traceTbl)
    }

    println(date, unionData.count())
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
    for (h <- 0 to hours - 1) {
      parts(h) = partitionPathFormat.format(cal.getTime)
      cal.add(Calendar.HOUR, 1)
    }
    "{" + parts.mkString(",") + "}"
  }

  /*
  reduce中as并非一定是as的日志，有可能也是event
   */
  def merge(as: UnionLog, event: UnionLog): UnionLog = {
    var log = as
    if (event.isshow == 1) {
      log = log.copy(
        isshow = event.isshow,
        show_timestamp = event.show_timestamp,
        show_network = event.show_network,
        show_ip = event.show_ip
      )
    }
    if (event.isclick == 1) {
      log = log.copy(
        isclick = event.isclick,
        click_timestamp = event.click_timestamp,
        antispam_score = event.antispam_score,
        antispam_rules = event.antispam_rules,
        click_network = event.click_network,
        click_ip = event.click_ip
      )
    }
    log
  }
}


