package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.CpcPartitioner
import com.cpc.spark.log.parser._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable


/**
  * Created by Roy on 2017/4/18.
  */
object MergeLog {

  var srcRoot = "/warehouse/dl_cpc.db"
  var prefix = ""
  var suffix = ""
  //  var srcRoot = "/gobblin/source/cpc"

  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  var g_date = new Date()

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
    prefix = args(4)
    suffix = args(5)
    val allTraceTbl = args(6) //cpc_all_trace_log


    val cpcPartitioner = new CpcPartitioner(1000)
    val cal = Calendar.getInstance()
    g_date = cal.getTime //以后只用这个时间
    cal.add(Calendar.HOUR, -hourBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)
    val spark = SparkSession.builder()
      .appName("union log %s partition = %s".format(table, partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
      .getOrCreate()

    var searchData = prepareSourceString(spark, "cpc_search_new", prefix + "cpc_search" + suffix, hourBefore, 1)
    if (searchData == null) {
      System.err.println("search data is empty")
      System.exit(1)
    }
    searchData.take(1).foreach {
      x =>
        println(x)
        println(LogParser.parseSearchLog(x))
    }
    val searchData2 = searchData
      .map(x => LogParser.parseSearchLog(x)) //(log)
      .filter(_ != null)
      .map(x => ((x.searchid, x.ideaid), x)) //((searchid,ideaid), Seq(log))
      .reduceByKey(cpcPartitioner, (x, y) => x) //去重
      .map { //覆盖时间，防止记日志的时间与flume推日志的时间不一致造成的在整点出现的数据丢失，下面的以search为准
      x =>
        var ulog = x._2.copy(date = date, hour = hour)
        ((ulog.searchid, ulog.ideaid), ulog)
    }

    val showData = prepareSourceString(spark, "cpc_show_new", prefix + "cpc_show" + suffix, hourBefore, 2)
    //    var showData2: rdd.RDD[(String, UnionLog)] = null
    //    showData2 = showData
    //      .map(x => LogParser.parseShowLog(x)) //(log)
    //      .filter(_ != null)
    //      .map(x => (x.searchid, x)) //(searchid, log)
    //      .reduceByKey((x, y) => x) //去重

    var showData2 = showData
      .map(x => LogParser.parseShowLog(x)) //(log)
      .filter(_ != null)
      .map(x => ((x.searchid, x.ideaid), Seq(x))) //((searchid,ideaid), Seq(log))
      .reduceByKey(cpcPartitioner, (x, y) => x ++ y)
      .map {
        x =>
          var log = x._2.head
          val logTime = log.ext("video_show_time").int_value
          x._2.foreach {
            y =>
              if (y.ext("video_show_time").int_value > logTime) {
                log = y
              }
          }
          ((log.searchid, log.ideaid), log)
      }


    val clickData = prepareSourceString(spark, "cpc_click_new", prefix + "cpc_click" + suffix, hourBefore, 2)
    var clickData2: rdd.RDD[((String, Int), UnionLog)] = null
    if (clickData != null) {
      clickData2 = clickData
        .map(x => LogParser.parseClickLog(x)) //(log)
        .filter(_ != null)
        .map(x => ((x.searchid, x.ideaid), Seq(x))) //((searchid,ideaid), Seq(log))
        .reduceByKey(cpcPartitioner, (x, y) => x ++ y).map {
        x => //((searchid,ideaid),seq())
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
          ((ulog.searchid, ulog.ideaid), ulog)
      }
    }

    val unionData1 = searchData2.leftOuterJoin(showData2).leftOuterJoin(clickData2)
      .map {
        x =>
          (x._1, (x._2._1._1, x._2._1._2, x._2._2)) //( (searchid,ideaid), (log1,log2,log3) )
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
          (log1.searchid, log1)
      }
    val unionData = unionData1.groupByKey(1000)
      .map { rec =>
        val logs = rec._2
        if (logs.size > 1) {
          var motive = Seq[Motivation]()
          val head = logs.head
          for (log <- logs) {
            val m = Motivation(log.userid, log.planid, log.unitid, log.ideaid, log.bid, log.price, log.isfill,
              log.isshow, log.isclick)
            motive = motive :+ m
          }
          head.copy(motive = motive)
        } else
          rec._2.head
      }

    spark.createDataFrame(unionData)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(table, date, hour))
    spark.sql(
      """
        |ALTER TABLE dl_cpc.%s add if not exists PARTITION(`date` = "%s", `hour` = "%s")
        | LOCATION  '/warehouse/dl_cpc.db/%s/date=%s/hour=%s'
      """.stripMargin.format(table, date, hour, table, date, hour))
    println("union done")

    val traceData = prepareSourceString(spark, "cpc_trace_new", prefix + "cpc_trace" + suffix, hourBefore, 2)
    if (traceData != null) {
      val trace = traceData.map(x => LogParser.parseTraceLog(x))
        .filter(x => x != null)
      println(trace.first())
      val click = unionData.filter(_.isclick > 0).map(x => (x.searchid, x.timestamp))
      val trace1 = trace.map(x => (x.searchid, x))
        .join(click)
        .map {
          x =>
            x._2._1.copy(search_timestamp = x._2._2, date = date, hour = hour)
        }
      spark.createDataFrame(trace1)
        .write
        .mode(SaveMode.Overwrite)
        .parquet("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(traceTbl, date, hour))
      spark.sql(
        """
          |ALTER TABLE dl_cpc.%s add if not exists PARTITION(`date` = "%s", `hour` = "%s")
          | LOCATION  '/warehouse/dl_cpc.db/%s/date=%s/hour=%s'
        """.stripMargin.format(traceTbl, date, hour, traceTbl, date, hour))
      println("trace_join done")
    }


    val traceall = prepareSourceString(spark, "cpc_trace_new", prefix + "cpc_trace" + suffix, hourBefore, 1)
      .map(x => LogParser.parseTraceLog(x))
      .filter(_ != null)
      .map(_.copy(date = date, hour = hour))
    spark.createDataFrame(traceall)
      .write
      .mode(SaveMode.Overwrite)
      .parquet("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(allTraceTbl, date, hour))
    spark.sql(
      """
        |ALTER TABLE dl_cpc.%s add if not exists PARTITION(`date` = "%s", `hour` = "%s")
        | LOCATION  '/warehouse/dl_cpc.db/%s/date=%s/hour=%s'
      """.stripMargin.format(allTraceTbl, date, hour, allTraceTbl, date, hour))
    println("trace_all done")

    spark.stop()
    for (i <- 0 until 50) {
      println("-")
    }
    println("MergeLog_done")
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
  def prepareSourceString(ctx: SparkSession, key: String, src: String, hourBefore: Int, hours: Int): rdd.RDD[String] = {
    val input = "%s/%s/%s/*".format(srcRoot, src, getDateHourPath(hourBefore, hours)) ///gobblin/source/cpc/cpc_search/{05,06...}
    println(input)
    val readData = ctx.read
      .parquet(input)
      .rdd
      .map {
        rec =>
          //val s = r.getMap[String, Row](2).getOrElse(key, null)
          val s = rec.getAs[Map[String, Row]]("field").getOrElse(key, null)
          val timestamp = rec.getAs[Long]("log_timestamp")

          if (s == null) {
            null
          } else {
            if (key == "cpc_show_new") {
              timestamp + s.getAs[String]("string_type")
            }
            else s.getAs[String]("string_type")
          }
      }
      .filter(_ != null)
    readData
  }

  /*
  cpc_search cpc_show cpc_click cpc_trace cpc_charge
   */
  def prepareSource(ctx: SparkSession, key: String, src: String, hourBefore: Int, hours: Int): rdd.RDD[Row] = {
    val input = "%s/%s/%s".format(srcRoot, src, getDateHourPath(hourBefore, hours)) ///gobblin/source/cpc/cpc_search/{05,06...}
    println(input)
    val baseData = ctx.read.schema(schema).parquet(input).repartition(1000)
    val tbl = "%s_data_%d".format(src, hourBefore)
    baseData.createTempView(tbl)
    ctx.sql("select field['%s'].string_type from %s".format(key, tbl)).rdd
  }

  def prepareTraceSource(src: rdd.RDD[Row]): rdd.RDD[TraceLog] = {
    src.map(x => LogParser.parseTraceLog(x.getString(0)))
      .filter(x => x != null && x.searchid.length > 5)
  }

  def getDateHourPath(hourBefore: Int, hours: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(g_date)
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












