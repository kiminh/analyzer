package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils
import com.cpc.spark.log.parser.{ExtValue, LogParser, TraceLog, UnionLog}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable


/**
  * Created by Roy on 2017/4/18.
  */
object AnalUnionLog4 {

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
    println("searchData", searchData.count())
    searchData.take(1).foreach {
      x =>
        println(x)

        println(x.getString(0))
        println(LogParser.parseSearchLog(x.getString(0)))
    }


    val searchData2: rdd.RDD[(String, UnionLog)] = searchData
      .map(x => LogParser.parseSearchLog(x.getString(0))) //(log)
      .filter(_ != null)
      .map(x => (x.searchid, x)) //(searchid, log)
      .reduceByKey((x, y) => x) //去重
      .map { //覆盖时间，防止记日志的时间与flume推日志的时间不一致造成的在整点出现的数据丢失，下面的以search为准
      x =>
        var ulog = x._2.copy(date = date, hour = hour)
        (x._1, ulog)
    }
    println("searchData2", searchData2.count())

    val showData = prepareSource(spark, "cpc_show", "src_cpc_show", hourBefore, 2)
    var showData2: rdd.RDD[(String, UnionLog)] = null
    if (showData != null) {
      showData2 = showData
        .map(x => LogParser.parseShowLog(x.getString(0))) //(log)
        .filter(_ != null)
        .map(x => (x.searchid, x)) //(searchid, log)
        .reduceByKey((x, y) => x) //去重
        .map {
        x =>
          (x._1, x._2)
      }
    }
    println("showData2", showData2.count())

    val clickData = prepareSource(spark, "cpc_click", "src_cpc_click", hourBefore, 2)
    var clickData2: rdd.RDD[(String, UnionLog)] = null
    if (clickData != null) {
      clickData2 = clickData
        .map(x => LogParser.parseClickLog(x.getString(0))) //(log)
        .filter(_ != null)
        .map(x => (x.searchid, Seq(x))) //(searchid, log)
        .reduceByKey {
        (x, y) =>
          x ++ y
      }.map {
        x => //(searchid,seq())
          var ulog = x._2.head
          var notgetGood = true
          var ext = mutable.Map[String, ExtValue]()
          x._2.foreach { //遍历log的seq
            log =>
              if (log.isSpamClick() == 1) {
                val spam = ext.getOrElse("spam_click", ExtValue())
                ext.update("spam_click", ExtValue(int_value = spam.int_value + 1))
                ulog = ulog.copy(
                  ext = ext
                )
              } else {
                if (notgetGood) {
                  ext.update("touch_x", log.ext("touch_x"))
                  ext.update("touch_y", log.ext("touch_y"))
                  ext.update("slot_width", log.ext("slot_width"))
                  ext.update("slot_height", log.ext("slot_height"))
                  ext.update("antispam_predict", log.ext("antispam_predict"))
                  ext.update("click_ua", log.ext("click_ua"))
                  ulog = ulog.copy(
                    isclick = log.isclick,
                    click_timestamp = log.click_timestamp,
                    antispam_score = log.antispam_score,
                    antispam_rules = log.antispam_rules,
                    click_network = log.click_network,
                    click_ip = log.click_ip,
                    ext = ext
                  )
                  notgetGood = false
                }
              }
          }
          (x._1, ulog)
      }
    }
    println("clickData2", clickData2.count())

    val unionData = searchData2.leftOuterJoin(showData2).leftOuterJoin(clickData2)
      .map {
        x =>
          (x._1, (x._2._1._1, x._2._1._2, x._2._2)) //( searchid, (log1,log2,log3) )
      }
      .map {
        x =>
          var log1 = x._2._1
          val log2 = x._2._2.getOrElse(null)
          val log3 = x._2._3.getOrElse(null)

          var ext1 = mutable.Map[String, ExtValue]() ++ log1.ext
          if (log2 != null) {
            if (log2.ext != null) {
              ext1 = ext1 ++ log2.ext
            }
            ext1.update("show_refer", log2.ext("show_refer"))
            ext1.update("show_ua", log2.ext("show_ua"))
            log1 = log1.copy(
              isshow = log2.isshow,
              show_timestamp = log2.show_timestamp,
              show_network = log2.show_network,
              show_ip = log2.show_ip,
              ext = ext1
            )
          }

          if (log3 != null) {
            if (log3.ext != null) {
              ext1 = ext1 ++ log3.ext
            }
            val spam = log3.ext.getOrElse("spam_click", ExtValue())
            ext1.update("spam_click", ExtValue(int_value = spam.int_value))
            log1 = log1.copy(
              ext = ext1
            )
            if (log3.isSpamClick() != 1) {
              ext1.update("touch_x", log3.ext("touch_x"))
              ext1.update("touch_y", log3.ext("touch_y"))
              ext1.update("slot_width", log3.ext("slot_width"))
              ext1.update("slot_height", log3.ext("slot_height"))
              ext1.update("antispam_predict", log3.ext("antispam_predict"))
              ext1.update("click_ua", log3.ext("click_ua"))
              log1 = log1.copy(
                isclick = log3.isclick,
                click_timestamp = log3.click_timestamp,
                antispam_score = log3.antispam_score,
                antispam_rules = log3.antispam_rules,
                click_network = log3.click_network,
                click_ip = log3.click_ip,
                ext = ext1
              )
            }
          }
          log1
      }

    val w = spark.createDataFrame(unionData)
      .write
      .mode(SaveMode.Append)
      .format("parquet")
      .partitionBy("date", "hour")
    //clear dir
    Utils.deleteHdfs("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(table, date, hour))
    w.saveAsTable("dl_cpc." + table)
    println("union", unionData.count())


    var search = unionData.map(x => (x.searchid, x.timestamp))

    val traceData1 = prepareSource(spark, "cpc_trace", "src_cpc_trace", hourBefore, 2)
    var traceData = prepareTraceSource(traceData1)

    if (traceData1 != null) {
      val trace = traceData.map(x => (x.searchid, x))
      val trace2 = trace.join(search)
//        .filter(x=>x._2._2.isDefined)
        .map {
          x =>
            val tlog = x._2._1
            tlog.copy(
              search_timestamp = x._2._2,
              date = date,
              hour = hour
            )
        }

      val w = trace2.toDF()
        .write
        .mode(SaveMode.Append)
        .format("parquet")
        .partitionBy("date", "hour")
      //clear dir
      Utils.deleteHdfs("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(traceTbl, date, hour))
      w.saveAsTable("dl_cpc." + traceTbl)
      println("trace", trace.count())
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
  def prepareSource(ctx: SparkSession, key: String, src: String, hourBefore: Int, hours: Int): rdd.RDD[Row] = {

    try {
      val input = "%s/%s/%s".format(srcRoot, src, getDateHourPath(hourBefore, hours)) ///gobblin/source/cpc/cpc_search/{05,06...}
      val baseData = ctx.read.schema(schema).parquet(input)
      val tbl = "%s_data_%d".format(src, hourBefore)
      baseData.createTempView(tbl)
      ctx.sql("select field['%s'].string_type from %s".format(key, tbl)).rdd
    } catch {
      case e: Exception => null
    }
  }

  def prepareTraceSource(src: rdd.RDD[Row]): rdd.RDD[TraceLog] = {
    src.map(x => LogParser.parseTraceLog(x.getString(0)))
      .filter(x => x != null && x.searchid.length > 0)
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