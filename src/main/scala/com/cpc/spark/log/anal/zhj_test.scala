package com.cpc.spark.log.anal

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.Event
import com.cpc.spark.log.parser._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable


/**
  * Created by Roy on 2017/4/18.
  */
object zhj_test {

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
    prefix = "src_"
    suffix = "_minute"
    val allTraceTbl = args(6) //cpc_all_trace_log


    val cal = Calendar.getInstance()
    g_date = cal.getTime //以后只用这个时间
    cal.add(Calendar.HOUR, -hourBefore) //hourBefore前的 时间
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime) //年月日
    val hour = new SimpleDateFormat("HH").format(cal.getTime) //小时

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()


    val showData = prepareSourceString(spark, "cpc_show_new")

    showData
      .map(x => (x, parseShowLog(x))) //(log)
      .filter { x => x._2._4 == "1026276" || x._2._5 == "1026276" }
      .filter(_._2._2 == "80000008")
      .collect()
      .foreach(println)

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

    spark, "cpc_search_new", "src_cpc_search_minute", hourBefore, 1
    spark, "cpc_show_new", "src_cpc_show_minute", hourBefore, 2
    读取数据
   */
  def prepareSourceString(ctx: SparkSession, key: String): rdd.RDD[String] = {
    val input = "/warehouse/dl_cpc.db/src_cpc_show_minute/2018-10-24/00/{00,10,20,30,40,50}/"
    println(input) // /warehouse/dl_cpc.db/src_cpc_search_minute/{2018-06-26/08}/*
    val readData = ctx.read
      .parquet(input)
      .rdd
      .map {
        rec =>
          //val s = r.getMap[String, Row](2).getOrElse(key, null)
          val s = rec.getAs[Map[String, Row]]("field").getOrElse(key, null) // key='cpc_search_new',..
        val timestamp = rec.getAs[Long]("log_timestamp")

          if (s == null) { //没有key 'cpc_search_new'
            null
          } else { //有
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

  //获取 {yyyy-MM-dd/HH,yyyy-MM-dd/HH}
  def getDateHourPath(hourBefore: Int, hours: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(g_date) //当前日期
    val parts = new Array[String](hours)
    cal.add(Calendar.HOUR, -hourBefore) //前一个小时 时间
    for (h <- 0 until hours) {
      parts(h) = partitionPathFormat.format(cal.getTime) //yyyy-MM-dd/HH
      cal.add(Calendar.HOUR, 1)
    }
    "{" + parts.mkString(",") + "}"
  }

  /**
    * 在hdfs上创建成功标记文件；unionlog, uniontracelog合并成功的标记文件
    *
    * @param mark
    */
  def createSuccessMarkHDFSFile(date: String, hour: String, mark: String): Unit = {
    val fileName = "/warehouse/cpc/%s/%s-%s.ok".format(mark, date, hour)
    val path = new Path(fileName)

    //get object conf
    val conf = new Configuration()
    //get FileSystem
    val fileSystem = FileSystem.newInstance(conf)

    try {
      val success = fileSystem.createNewFile(path)
      if (success) {
        println("create file success")
      }
    } catch {
      case e: IOException => e.printStackTrace()
    } finally {
      try {
        if (fileSystem != null) {
          fileSystem.close()
        }
      } catch {
        case e: IOException => e.printStackTrace()
      }
    }
  }

  case class SrcExtValue(int_type: Int = 0, long_type: Long = 0, float_type: Float = 0, string_type: String = "")

  def parseShowLog(txt: String): (String, String, String, String, String) = {
    val data = Event.parse_show_log(txt)
    if (data != null) {
      val body = data.event
      (body.getSearchId, body.getMedia.getMediaId, body.getMedia.getAdslotType.getNumber.toString,
        body.getMedia.getAdslotId, body.getDspInfo.getAdslotId)
    } else ("", "", "", "", "")
  }
}












