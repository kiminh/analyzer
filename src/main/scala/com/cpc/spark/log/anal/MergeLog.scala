package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import aslog.Aslog
import com.cpc.spark.log.parser.{LogParser, UnionLog}
import org.apache.spark.rdd
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._


/**
  * Created by Roy on 2017/4/18.
  */
object MergeLog {

  def main(args: Array[String]): Unit = {
    System.exit(1)
    if (args.length < 3) {
      System.err.println(s"""
        |Usage: MergeLog <hdfs_input> <hdfs_ouput> <hour_before>
        |
        """.stripMargin)
      System.exit(1)
    }

    srcRoot = args(0)
    val table = args(1)
    val hourBefore = args(2).toInt

    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore.toInt)
    val date = LogParser.dateFormat.format(cal.getTime)
    val hour = LogParser.hourFormat.format(cal.getTime)
    val spark = SparkSession.builder()
      .appName("cpc union log merge to %s topc = %s/%s".format(table, date, hour))
      .enableHiveSupport()
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._

    var unionData = prepareSource(spark, "cpc_search", hourBefore.toInt, 1)
    if (unionData == null) {
      System.err.println("can not load search data")
      System.exit(1)
    }

    val showData = prepareSource(spark, "cpc_show", hourBefore.toInt, 2)
    if (showData != null) {
      unionData = unionData.union(showData)
    }

    val clickData = prepareSource(spark, "cpc_click", hourBefore.toInt, 2)
    if (clickData != null) {
      unionData = unionData.union(clickData)
    }

    val traceData = prepareSource(spark, "cpc_trace", hourBefore.toInt, 2)
    if (traceData != null) {
      unionData = unionData.union(clickData)
    }

    //val chargeData = prepareSource(spark, "cpc_charge", date, hour)

    unionData.filter(x => x.searchid.length > 0)
      .map(x => (x.searchid, x))
      .reduceByKey{
        (x, y) =>
          if (y.timestamp > 0) {
            merge(y, x)
          } else {
            merge(x, y)
          }
      }.map(x => x._2)
      .filter(x => x.timestamp > 0)

    spark.createDataFrame(unionData)
      .select("*")
      .write
      .mode(SaveMode.Append)
      .format("parquet")
      .partitionBy("date", "hour")
      .saveAsTable("dl_cpc." + table)

    /*
    spark.sql("create TABLE if not exists dl_cpc.cpc_union_log_tmp like union_log_temp")
    spark.sql("INSERT INTO TABLE dl_cpc.cpc_union_log_tmp select * from union_log_temp")
    createTable(spark)
    spark.sql(s"""
      |INSERT INTO TABLE dl_cpc.cpc_union_log PARTITION(`date` = "%s", `hour` = "%s")
      |SELECT * FROM union_log_temp
       """.stripMargin.format(date, hour))
       */
  }

  val schema = StructType(Array(
    StructField("log_timestamp",LongType,true),
    StructField("ip",StringType,true),
    StructField("field",MapType(StringType,
      StructType(Array(
        StructField("int_type",IntegerType,true),
        StructField("long_type",LongType,true),
        StructField("float_type",FloatType,true),
        StructField("string_type",StringType,true))),true),true)))

  var srcRoot = "/gobblin/source/cpc"

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
    }catch {
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
      val date = LogParser.dateFormat.format(cal.getTime)
      val hour = LogParser.hourFormat.format(cal.getTime)
      parts(h) = date + "/" + hour
    }
    "{" + parts.mkString(",") + "}"
  }

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
    var cols = new Array[String](colsRdd.count().toInt - 2)
    var n = 0
    colsRdd.collect().foreach {
      col =>
        val v = col.toString.stripPrefix("[").stripSuffix("]").split(",")
        if (v(0) != "date" && v(0) != "hour") {
          cols(n) = "`%s` %s".format(v(0), v(1).toUpperCase)
        }
        n += 1
    }

    ctx.sql(s"""
       |CREATE TABLE IF NOT EXISTS dl_cpc.cpc_union_log (%s)
       |PARTITIONED BY (`date` STRING, `hour` STRING) LOCATION
       """.stripMargin.format(cols.mkString(",")))
  }
}



