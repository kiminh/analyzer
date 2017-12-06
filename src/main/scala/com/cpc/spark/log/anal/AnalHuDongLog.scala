package com.cpc.spark.log.anal

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.antispam.log.GetAntispamLog.{mariadbProp, mariadbUrl}
import com.cpc.spark.common.Utils
import com.cpc.spark.log.parser.{HuDongLog, LogParser, TraceLog, UnionLog}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}


/**
  * Created by Roy on 2017/4/18.
  */
object AnalHuDongLog {

  var srcRoot = "/gobblin/source/cpc"

  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  var mariadbUrl = ""
  val mariadbProp = new Properties()

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
    val hourBefore = args(1).toInt
    val logType = args(2)
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)
    val spark = SparkSession.builder()
      .appName("get hudong log %s".format( partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
      .getOrCreate()

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.union_write.url")
    mariadbProp.put("user", conf.getString("mariadb.union_write.user"))
    mariadbProp.put("password", conf.getString("mariadb.union_write.password"))
    mariadbProp.put("driver", conf.getString("mariadb.union_write.driver"))

    val logTypeArr = logType.split(",")
    if(logTypeArr.length <= 0){
      System.err.println(
        s"""logTypeArr error
        """)
      System.exit(1)
    }
    println("logType :"+logType)
    val traceData = prepareSource(spark, "cpc_trace", hourBefore, 1)
    if (traceData == null) {
        spark.stop()
        System.exit(1)
    }
    var hudongLog = prepareTraceSource(traceData).map(x => x.copy( date = date, hour = hour)).filter{
      x =>
        var flag = false
        logTypeArr.foreach{
          logType => if(logType == x.log_type){ flag = true}
        }
        flag
    }.map{
      x =>
        ((x.adslot_id,x.log_type,x.date,x.hour),1)
    }.reduceByKey((x,y) => x+y).map{
      case ((adslot_id,log_type,date1,hour1),count) =>
        HuDongLog(adslot_id,logType,date1,hour1,count)
    }

    println("hudonglog", hudongLog.count())
    clearReportHourData("report_hudong", date,hour)
    spark.createDataFrame(hudongLog)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "union.report_hudong", mariadbProp)
    spark.stop()
  }

  def clearReportHourData(tbl: String, date: String, hour:String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report.%s where `date` = "%s" and hour ="%s"
        """.stripMargin.format(tbl, date, hour)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e)
    }
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
  def prepareTraceSource(src: rdd.RDD[Row]): rdd.RDD[HuDongLog] = {
    src.map(x => LogParser.parseHuDongTraceLog(x.getString(0)))
      .filter(x => x != null && x.adslot_id.length >0 )

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


