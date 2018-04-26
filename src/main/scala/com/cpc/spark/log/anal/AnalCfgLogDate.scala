package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils
import com.cpc.spark.log.parser.{LogParser, TraceLog, UnionLog}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


/**
  * Created by Roy on 2017/4/18.
  */
object AnalCfgLogDate {

  var srcRoot = "/gobblin/source/cpc"

  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println(
        s"""
           |usage: analunionlog <hdfs_input>  <hour>
           |
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    srcRoot = args(0)
    val dateBefore = args(1).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -dateBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val table ="cpc_cfg_log"
    val spark = SparkSession.builder()
      .appName("cpc anal cfg log date %s partition = %s".format(table, partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
      .getOrCreate()

    val sql = "SELECT thedate,thehour, field['cpc_cfg'].string_type from src_cpc.cpc_cfg  where thedate = \"%s\" ".format(date)
    println(sql)
    val cfgRdd = spark.sql(sql).rdd.map {
      x =>
        try {
          val date = x(0).toString()
          var hour = x(1).toString()
          val  cfg = x(2).toString()
          (date, hour, cfg)
        }catch {
          case e: Exception =>
            null
        }
    }.filter(x => x!= null)


    val cfglog =  cfgRdd.map(x => LogParser.parseCfgLog(x._3)).filter(x => x != null && x.date == date )
    //clear dir
    var hour = 0
    var thehour = ""
    for(hour <- 0 to 23){
      if(hour < 10){
        thehour = "0"+hour
      }else{
        thehour = hour.toString
      }
      var toData = cfglog.filter(x => x.hour == thehour)
      Utils.deleteHdfs("/warehouse/dl_cpc.db/%s/date=%s/hour=%s".format(table, date, thehour))
      spark.createDataFrame(toData)
        .write
        .mode(SaveMode.Append)
        .format("parquet")
        .partitionBy("date", "hour")
        .saveAsTable("dl_cpc." + table)
       println("hour",thehour)
       println("cfglog", toData.count())
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


