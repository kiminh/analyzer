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

  //  var srcRoot = "/gobblin/source/cpc"
  var srcRoot = "/warehouse/dl_cpc.db"

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
    val logTypeStr = args(2)
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)

    val spark = SparkSession.builder()
      .appName("get hudong log %s".format(partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
      .getOrCreate()

    val conf = ConfigFactory.load()
    mariadbUrl = conf.getString("mariadb.union_write.url")
    mariadbProp.put("user", conf.getString("mariadb.union_write.user"))
    mariadbProp.put("password", conf.getString("mariadb.union_write.password"))
    mariadbProp.put("driver", conf.getString("mariadb.union_write.driver"))

    val logTypeArr = logTypeStr.split(",")
    if (logTypeArr.length <= 0) {
      System.err.println(
        s"""logTypeArr error
        """)
      System.exit(1)
    }
    println("logType :" + logTypeArr)

    //    val traceData = prepareSourceString(spark, "cpc_trace", "src_cpc_trace_minute", hourBefore, 1)
    //    if (traceData == null) {
    //        spark.stop()
    //        System.exit(1)
    //    }

    var hudongLog = spark.sql(
      """
        |SELECT trace_type, adslot_id
        |FROM dl_cpc.logparsed_cpc_trace_minute
        |WHERE thedate="%s" AND thehour="%s"
      """.stripMargin.format(date, hour))
      .rdd
      .filter {
        x =>
          var flag = false
          logTypeArr.foreach {
            logType =>
              if (logType == x.getAs[String]("trace_type")) {
                flag = true
              }
          }
          flag
      }.map {
      x =>
        ((x.getAs[Int]("adslot_id"), x.getAs[String]("trace_type"), date, hour), 1)
    }.reduceByKey((x, y) => x + y).map {
      case ((adslot_id, log_type, date1, hour1), count) =>
        HuDongLog(adslot_id, log_type, date1, hour1, count)
    }
      .filter(x => x.adslot_id > 0)
    println(11111)
    clearReportHourData("report_hudong", date, hour)
    spark.createDataFrame(hudongLog)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "union.report_hudong", mariadbProp)
    spark.stop()
    for (h <- 0 until 50) {
      println("-")
    }
    println("AnalHuDongLog_done")
  }

  def clearReportHourData(tbl: String, date: String, hour: String): Unit = {
    try {
      Class.forName(mariadbProp.getProperty("driver"))
      val conn = DriverManager.getConnection(
        mariadbUrl,
        mariadbProp.getProperty("user"),
        mariadbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from union.%s where `date` = "%s" and hour ="%s"
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


}


