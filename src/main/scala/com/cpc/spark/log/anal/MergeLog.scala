package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.log.parser.{LogParser, UnionLog}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types._


/**
  * Created by Roy on 2017/4/18.
  */
object MergeLog {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(s"""
        |Usage: MergeLog <hdfs_input> <hdfs_ouput> <hour_before>
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(input, output, hourBefore) = args
    val cal = Calendar.getInstance()
    cal.add(Calendar.HOUR, -hourBefore.toInt)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    val hour = new SimpleDateFormat("HH").format(cal.getTime)
    val spark = SparkSession.builder()
      .appName("cpc union log merge to %s %s %s".format(output, date, hour))
      .enableHiveSupport()
      //.config("spark.some.config.option", "some-value")
      .getOrCreate()


    import spark.implicits._
    val search_input = input + "/cpc_search/" + date + "/" + hour
    val show_input = input + "/cpc_show/" + date + "/" + hour
    val click_input = input + "/cpc_click/" + date + "/" + hour
    //val charge_input = input + "/cpc_charge/" + date + "/" + hour

    val fields = Array(
      StructField("log_timestamp",LongType,true),
      StructField("ip",StringType,true),
      StructField("field",MapType(StringType,
        StructType(Array(
          StructField("int_type",IntegerType,true),
          StructField("long_type",LongType,true),
          StructField("float_type",FloatType,true),
          StructField("string_type",StringType,true))),true),true))

    val schema= StructType(fields)
    val showBaseData = spark.read.schema(schema).parquet(show_input)
    val clickBaseData = spark.read.schema(schema).parquet(click_input)
    val searchBaseData = spark.read.schema(schema).parquet(search_input)
    //val chargeBaseData = spark.read.schema(schema).parquet(charge_input)

    searchBaseData.createTempView("search_data")
    showBaseData.createTempView("show_data")
    clickBaseData.createTempView("click_data")
    //chargeBaseData.createTempView("charge_data")

    val searchData = spark.sql("select field['cpc_search'].string_type from search_data")
      .rdd.map(x => LogParser.parseSearchLog(x.getString(0)))
    val showData = spark.sql("select field['cpc_show'].string_type from show_data")
      .rdd.map(x => LogParser.parseShowLog(x.getString(0)))
    val clickData = spark.sql("select field['cpc_click'].string_type from click_data")
      .rdd.map(x => LogParser.parseClickLog(x.getString(0)))

    val unionData = searchData
      .union(showData)
      .union(clickData)
      .filter(x => x.searchid.length > 0)
      .map(x => (x.searchid, x))
      .reduceByKey{
        (x, y) =>
          if (y.timestamp > 0) {
            merge(y, x)
          } else {
            merge(x, y)
          }
      }.map{
        x =>
          x._2.copy(
            date = date,
            hour = hour
          )
      }


    spark.createDataFrame(unionData).select("*")
      .write
      .mode(SaveMode.Append)
      .format("parquet")
      .partitionBy("date", "hour")
      .saveAsTable("dl_cpc." + output)

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



