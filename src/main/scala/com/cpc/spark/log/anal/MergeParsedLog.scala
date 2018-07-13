package com.cpc.spark.log.anal

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}


/**
  * /warehouse/dl_cpc.db/logparsed_cpc_search_minute
  * /warehouse/dl_cpc.db/logparsed_cpc_click_minute
  * /warehouse/dl_cpc.db/logparsed_cpc_show_minute
  * (searchid,ideaid)
  */
object MergeParsedLog {

  //数据源根目录
  var srcRoot = ""
  var prefix = ""
  var suffix = ""

  //时间格式
  val partitionPathFormat = new SimpleDateFormat("yyyy-MM-dd/HH")

  //当前日期
  var g_date = new Date()

  def main(args: Array[String]): Unit = {
    //参数不对
    if (args.length < 3) {
      System.err.println(
        s"""
           |usage: analunionlog <hdfs_input> <hdfs_ouput> <hour_before>
         """.stripMargin
      )
      System.exit(1)
    }

    //设置日志级别
    Logger.getRootLogger.setLevel(Level.WARN)

    srcRoot = args(0)
    val mergeTbl = args(1)
    val hourBefore = args(2).toInt
    prefix = args(3)
    suffix = args(4)

    val cal = Calendar.getInstance()
    g_date = cal.getTime //以后只用这个时间
    cal.add(Calendar.HOUR, -hourBefore) //hourBefore前的 时间
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime) //年月日
    val hour = new SimpleDateFormat("HH").format(cal.getTime) //小时

    //获得sparksession
    val spark = SparkSession.builder()
      .appName("union log %s partition = %s".format(mergeTbl, partitionPathFormat.format(cal.getTime)))
      .enableHiveSupport()
      .getOrCreate()

    // search
    var searchRDD = prepareSourceString(spark, "cpc_search_new", prefix + "cpc_search" + suffix, hourBefore, 1)




    //show
    var showRDD = prepareSourceString(spark, "cpc_show_new", prefix + "cpc_show" + suffix, hourBefore, 2)

    //click
    var clickRDD = prepareSourceString(spark, "cpc_click_new", prefix + "cpc_click" + suffix, hourBefore, 2)



  }


  /**
    * 获得数据
    */
  def prepareSourceString(ctx: SparkSession, key: String, src: String, hourBefore: Int, hours: Int): RDD[Row] = {
    val input = "%s/%s/%s/*".format(srcRoot, src, getDateHourPath(hourBefore, hours))
    println(input) // /warehouse/dl_cpc.db/src_cpc_search_minute/{2018-06-26/08}/*
    val readData = ctx.read
      .parquet(input)
      .rdd
      .filter(_ != null)

    readData
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
}
