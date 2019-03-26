package com.cpc.spark.OcpcProtoType.report_qtt

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object GetUnitCostofElds {
  def main(args: Array[String]): Unit = {
    val dateBuffer = getDate("2019-03-01", "2019-03-25")
    val spark = SparkSession.builder().appName("getUnitCostOfElds").enableHiveSupport().getOrCreate()
    getUnitCost(dateBuffer, spark)
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

  def getUnitCost(dateBuffer: ListBuffer[String], spark: SparkSession): Unit ={
    for(date <- dateBuffer){
      val sql =
        s"""
          |select
          |    unitid,
          |    sum(case when isclick = 1 then price else 0 end) as unit_all_cost
          |from
          |    dl_cpc.ocpc_base_unionlog
          |where
          |    `date` = '$date'
          |and
          |    (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
          |and
          |    media_appsid  in ("80000001", "80000002")
          |and
          |    isshow = 1
          |and
          |    antispam = 0
          |and
          |    adsrc = 1
          |group by
          |    unitid
        """.stripMargin
      val dataDF = spark.sql(sql)
      dataDF
        .withColumn("date", lit(date))
        .repartition(50)
        .write.mode("overwrite").insertInto("test.wt_unit_cost_every_day_elds")
      println(date)
    }
  }
}
