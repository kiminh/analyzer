package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar
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
    val spark = SparkSession.builder()
      .appName("cpc anal union log %s partition = %s".format(table, partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
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
      .filter(_.timestamp > 0)

    //write union log data
    spark.createDataFrame(unionData)
      .write
      .mode(SaveMode.Append)
      .format("parquet")
      .partitionBy("date", "hour")
      .saveAsTable("dl_cpc." + table)

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
    if (event.duration > 0 && event.duration > log.duration) {
      log = log.copy(duration = event.duration)
    }
    log
  }
}


