package com.cpc.spark.OcpcProtoType.report_qtt

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object GetJianZhanSearchid {
  def main(args: Array[String]): Unit = {
    val dateBuffer = getDate("2019-03-23", "2019-03-28")
    val spark = SparkSession.builder().appName("GetJianZhanSearchid").enableHiveSupport().getOrCreate()
    getSearchid(dateBuffer, spark)
  }

  def getDate(startDate: String, endDate: String): ListBuffer[String] = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    var dateBuffer = new ListBuffer[String]
    val startTimeStamp = sdf.parse(startDate).getTime
    val endTimeStamp = sdf.parse(endDate).getTime
    val n = ((endTimeStamp - startTimeStamp) / (24 * 60 * 60 * 1000)).toInt
    for(i <- 0 to n){
      val tempDate = sdf.format(new Date(startTimeStamp + i * 24 * 60 * 60 * 1000))
      dateBuffer.append(tempDate.toString)
    }
    dateBuffer
  }

  // 获取建站searchid
  def getSearchid(dateBuffer: ListBuffer[String], spark: SparkSession): Unit ={
    for(date <- dateBuffer){
      val sql =
        s"""
          |select
          |    searchid
          |from
          |    dl_cpc.cpc_basedata_union_events
          |where
          |    siteid > 0
          |and
          |    day = '$date'
          |and
          |    media_appsid  in ("80000001", "80000002")
          |and
          |    adsrc = 1
          |and
          |    (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
          |group by
          |   day,
          |   searchid
        """.stripMargin
      println("--------------------------------")
      println(sql)
      val searchidDF = spark.sql(sql)
      searchidDF
        .withColumn("date", lit(date))
        .repartition(50)
        .write.mode("overwrite").insertInto("test.wt_jianzhan_searchids")
      println(date)
      println("--------------------------------")
    }
  }

}
