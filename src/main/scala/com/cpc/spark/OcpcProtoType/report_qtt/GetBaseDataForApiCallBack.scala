package com.cpc.spark.OcpcProtoType.report_qtt

import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object GetBaseDataForApiCallBack {
  def main(args: Array[String]): Unit = {
    val dateBuffer = getDate("2019-03-20", "2019-03-27")
    val spark = SparkSession.builder().appName("BaseDataForApiCallBack").enableHiveSupport().getOrCreate()
    for(date <- dateBuffer){
      getBaseData(date, spark)
      println(date)
    }
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

  def getBaseData(date: String, spark: SparkSession): Unit ={
    val sql =
      s"""
        |select
        |    searchid,
        |    unitid,
        |    isclick,
        |    ocpc_log
        |from
        |    dl_cpc.ocpc_base_unionlog
        |where
        |    `date` = '$date'
        |and
        |    length(ocpc_log) > 0
        |and
        |    media_appsid  in ("80000001", "80000002")
        |and
        |    isshow = 1
        |and
        |    antispam = 0
        |and
        |    adsrc = 1
      """.stripMargin
    val data = spark.sql(sql)
    data
      .withColumn("ocpc_log_dict",udfStringToMap()(col("ocpc_log")))
      .withColumn("date", lit(date))
      .repartition(50)
      .write.mode("overwrite").insertInto("test.wt_api_callback_index_from_base")
  }
}
