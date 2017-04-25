package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

import com.cpc.spark.log.parser.{LogParser, UnionLog}
import org.apache.spark.rdd
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._


/**
  * Created by Roy on 2017/4/18.
  */
object AnalUnionLog {

  var srcRoot = "/gobblin/source/cpc"

  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  val mariadbUrl = "jdbc:mysql://139.224.232.57:3306/report"

  val mariadbProp = new Properties()

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: AnalUnionLog <hdfs_input> <hdfs_ouput> <hour_before>
           |
        """.stripMargin)
      System.exit(1)
    }
    srcRoot = args(0)
    val table = args(1)
    val hourBefore = args(2).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore)

    mariadbProp.put("user", "report")
    mariadbProp.put("password", "report!@#")
    mariadbProp.put("driver", "org.mariadb.jdbc.Driver")

    val spark = SparkSession.builder()
      .appName("cpc anal union log %s partition = %s".format(table, partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

    var unionData = prepareSource(spark, "cpc_search", hourBefore, 1)
    if (unionData == null) {
      System.err.println("can not load search data")
      System.exit(1)
    }

    val showData = prepareSource(spark, "cpc_show", hourBefore, 2)
    if (showData != null) {
      unionData = unionData.union(showData)
    }

    val clickData = prepareSource(spark, "cpc_click", hourBefore, 2)
    if (clickData != null) {
      unionData = unionData.union(clickData)
    }

    val traceData1 = prepareSource(spark, "cpc_trace", hourBefore, 1)
    if (traceData1 != null) {
      unionData = unionData.union(traceData1)
    }
    val traceData2 = prepareSource(spark, "cpc_trace", hourBefore, 1)
    if (traceData2 != null) {
      unionData = unionData.union(traceData2)
    }

    unionData = unionData.filter(_.searchid.length > 0)
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
      .filter(_.timestamp > 0)
      .cache()

    //write union log data
    spark.createDataFrame(unionData)
      .select("*")
      .write
      .mode(SaveMode.Append)
      .format("parquet")
      .partitionBy("date", "hour")
      .saveAsTable("dl_cpc." + table)

    //write hourly data to mysql
    val chargeData = unionData.map {
        x =>
          val charge = MediaChargeReport(
            media_id = x.media_appsid.toInt,
            adslot_id = x.adslotid.toInt,
            unit_id = x.unitid,
            idea_id = x.ideaid,
            plan_id = x.planid,
            request = 1,
            served_request = x.isfill,
            impression = x.isshow,
            click = x.isclick,
            charged_click = x.isCharged(),
            spam_click = x.isSpamClick(),
            cash_cost = x.price,
            date = x.date,
            hour = x.hour.toInt
          )
          (charge.key, charge)
      }
      .reduceByKey((x, y) => x.sum(y))
      .map(_._2)

    spark.createDataFrame(chargeData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_charge_hourly", mariadbProp)

    val geoData = unionData.map {
      x =>
        val report = MediaGeoReport(
          media_id = x.media_appsid.toInt,
          adslot_id = x.adslotid.toInt,
          unit_id = x.unitid,
          idea_id = x.ideaid,
          plan_id = x.planid,
          country = x.country,
          province = x.province,
          city = x.city,
          request = 1,
          served_request = x.isfill,
          impression = x.isshow,
          click = x.isclick,
          charged_click = x.isCharged(),
          spam_click = x.isSpamClick(),
          cash_cost = x.price,
          date = x.date,
          hour = x.hour.toInt
        )
        (report.key, report)
      }
      .reduceByKey((x, y) => x.sum(y))
      .map(_._2)

    spark.createDataFrame(geoData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_geo_hourly", mariadbProp)

    val osData = unionData.map {
      x =>
        val report = MediaOsReport(
          media_id = x.media_appsid.toInt,
          adslot_id = x.adslotid.toInt,
          unit_id = x.unitid,
          idea_id = x.ideaid,
          plan_id = x.planid,
          os_type = x.os,
          request = 1,
          served_request = x.isfill,
          impression = x.isshow,
          click = x.isclick,
          charged_click = x.isCharged(),
          spam_click = x.isSpamClick(),
          cash_cost = x.price,
          date = x.date,
          hour = x.hour.toInt
        )
        (report.key, report)
      }
      .reduceByKey((x, y) => x.sum(y))
      .map(_._2)

    spark.createDataFrame(osData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_os_hourly", mariadbProp)

    spark.createDataFrame(osData)
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadbUrl, "report.report_media_os_hourly", mariadbProp)

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
  def prepareSource(ctx: SparkSession, src: String, hourBefore: Int, hours: Int): rdd.RDD[UnionLog] = {
    try {
      val input = "%s/%s/%s".format(srcRoot, src, getDateHourPath(hourBefore, hours))
      val baseData = ctx.read.schema(schema).parquet(input)
      baseData.createTempView(src + "_data")
      val rddData = ctx.sql("select field['%s'].string_type from %s_data".format(src, src)).rdd
      src match {
        case "cpc_search" => rddData.map(x => LogParser.parseSearchLog(x.getString(0)))
        case "cpc_show" => rddData.map(x => LogParser.parseShowLog(x.getString(0)))
        case "cpc_click" => rddData.map(x => LogParser.parseClickLog(x.getString(0)))
        case "cpc_trace" => rddData.map(x => LogParser.parseTraceLog(x.getString(0)))
        case _ => null
      }
    } catch {
      case e: Exception =>
        null
    }
  }

  def getDateHourPath(hourBefore: Int, hours: Int): String = {
    val cal = Calendar.getInstance()
    val parts = new Array[String](hours)
    cal.add(Calendar.HOUR, -hourBefore)
    for (h <- 0 to hours - 1) {
      cal.add(Calendar.HOUR, h)
      parts(h) = partitionPathFormat.format(cal.getTime)
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
    if (event.duration > 0 && event.duration > log.duration) {
      log = log.copy(duration = event.duration)
    }
    log
  }

  def createTable(ctx: SparkSession): Unit = {
    val colsRdd = ctx.sql("describe union_log_temp").queryExecution.toRdd
    val cols = new Array[String](colsRdd.count().toInt - 2)
    var n = 0
    colsRdd.collect().foreach {
      col =>
        val v = col.toString.stripPrefix("[").stripSuffix("]").split(",")
        if (v(0) != "date" && v(0) != "hour") {
          cols(n) = "`%s` %s".format(v(0), v(1).toUpperCase)
        }
        n += 1
    }

    ctx.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS dl_cpc.cpc_union_log (%s)
         |PARTITIONED BY (`date` STRING, `hour` STRING) LOCATION
       """.stripMargin.format(cols.mkString(",")))
  }
}


