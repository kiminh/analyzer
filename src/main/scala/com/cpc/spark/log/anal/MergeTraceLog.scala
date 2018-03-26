package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils
import com.cpc.spark.log.parser.{ExtValue, LogParser, TraceLog, UnionLog}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}



/**
  * Created by Roy on 2017/4/18.
  */
object MergeTraceLog {

  var srcRoot = "/warehouse/dl_cpc.db"
  //  var srcRoot = "/gobblin/source/cpc"

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

    var searchData = prepareSource(spark, "cpc_search", "src_cpc_search", hourBefore, 1)
    if (searchData == null) {
      System.err.println("search data is empty")
      System.exit(1)
    }
    searchData.take(1).foreach {
      x =>
        println(x)
        println(LogParser.parseSearchLog(x))
    }
    val searchData2: rdd.RDD[(String, UnionLog)] = searchData
      .map(x => LogParser.parseSearchLog(x)) //(log)
      .filter(_ != null)
      .map(x => (x.searchid, x)) //(searchid, log)
      .reduceByKey((x, y) => x) //去重
      .map { //覆盖时间，防止记日志的时间与flume推日志的时间不一致造成的在整点出现的数据丢失，下面的以search为准
      x =>
        var ulog = x._2.copy(date = date, hour = hour)
        (x._1, ulog)
    }

    val traceData = prepareSource(spark, "cpc_trace", "src_cpc_trace", hourBefore, 2)
    if (traceData != null) {
      val trace = traceData.map(x => LogParser.parseTraceLog(x))
        .filter(x => x != null && x.searchid.length > 5)
      println(trace.first())
      val search = searchData2
        .map {
          x =>
            (x._1, (x._2.timestamp, Seq[TraceLog]()))
        }
      val trace1 = trace.map(x => (x.searchid, Seq(x)))
        .reduceByKey(_ ++ _)
        .repartition(1000)
        .map(x => (x._1, (-1, x._2)))
        .union(search)
        .reduceByKey {
          (x, y) =>
            if (x._1 >= 0) {
              (x._1, y._2)
            } else {
              (y._1, x._2)
            }
        }
        .filter(_._2._1 >= 0)
        .flatMap {
          x =>
            val t = x._2._1
            x._2._2.map {
              v =>
                v.copy(
                  search_timestamp = t,
                  date = date,
                  hour = hour
                )
            }
        }

      val w = trace1.toDF()
        .write
        .mode(SaveMode.Append)
        .format("parquet")
        .partitionBy("date", "hour")
      //clear dir
      Utils.deleteHdfs("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(traceTbl, date, hour))
      w.saveAsTable("dl_cpc." + traceTbl)
      println("trace", trace1.count())
    }
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
  def prepareSource(ctx: SparkSession, key: String, src: String, hourBefore: Int, hours: Int): rdd.RDD[String] = {
    val input = "%s/%s/%s".format(srcRoot, src, getDateHourPath(hourBefore, hours)) ///gobblin/source/cpc/cpc_search/{05,06...}
    import ctx.implicits._
    println(input)
    ctx.read
      .parquet(input)
      .rdd
      .map {
        r =>
          //val s = r.getMap[String, Row](2).getOrElse(key, null)
          val s = r.getAs[Map[String, Row]]("field").getOrElse(key, null)

          if (s == null) {
            null
          } else {
            s.getAs[String]("string_type")
          }
      }
      .filter(_ != null)
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

  case class SrcExtValue(int_type: Int = 0, long_type: Long = 0, float_type: Float = 0, string_type: String = "")

}