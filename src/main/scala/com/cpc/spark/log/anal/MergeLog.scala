package com.cpc.spark.log.anal

import com.cpc.spark.common.Ui
import com.cpc.spark.log.parser.{LogParser, UnionLog}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


/**
  * Created by Roy on 2017/4/18.
  */
object MergeLog {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(s"""
        |Usage: MergeLog <hdfs_input> <hdfs_output> <date> <hour>
        |
        """.stripMargin)
      System.exit(1)
    }

    val Array(input, output, date, hour) = args
    val spark = SparkSession.builder()
      .appName("SparkSQL Anal date=" + date + "/" + hour)
      //      .config("spark.some.config.option", "some-value")
      .getOrCreate()
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

    val searchRDD = spark.sql("select field['cpc_search'].string_type from search_data").rdd
    val searchData = searchRDD.map(x => LogParser.parseSearchLog(x.getString(0)))

    val showRDD = spark.sql("select field['cpc_show'].string_type from show_data").rdd
    val showData = showRDD.map(x => LogParser.parseEventLog(x.getString(0)))

    val clickRDD = spark.sql("select field['cpc_click'].string_type from click_data").rdd
    val clickData = clickRDD.map(x => LogParser.parseEventLog(x.getString(0)))

    val result = searchData
      .union(showData)
      .union(clickData)
      .filter(x => x.searchid.length > 0)
      .map(x => (x.searchid, x))
      .reduceByKey{
        (x, y) =>
          var log = x
          if (y.timestamp > 0) {
            log = log.copy(
              mediaType = y.mediaType,
              mediaAppsid = y.mediaAppsid,
              adslotid = y.adslotid,
              adslotType = y.adslotType,
              adnum = y.adnum,
              isfill = y.isfill,
              ideaid = y.ideaid,
              unitid = y.unitid,
              planid = y.planid,
              country = y.country,
              province = y.province,
              city = y.city,
              locisp = y.locisp,
              uid = y.uid,
              ua = y.ua,
              os = y.os,
              screenw = y.screenw,
              screenh = y.screenh
            )
          }
          if (y.showtime > 0 && y.showtime < log.showtime) {
            log = log.copy(
              isshow = y.isshow,
              showtime = y.showtime
            )
          }
          if (y.clicktime > 0 && y.clicktime < log.showtime) {
            log = log.copy(
              isclick = y.isclick,
              clicktime = y.clicktime,
              antispamScore = y.antispamScore,
              antispamRules = y.antispamRules
            )
          }
          log
      }.map(x => x._2)
      .filter(x => x.timestamp > 0)

    println("---------------------", result.count())
    result.collect().take(100).foreach {
      x =>
        print("| " + x.toString)
    }
  }
}
