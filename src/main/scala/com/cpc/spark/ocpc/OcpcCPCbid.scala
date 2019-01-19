package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.ocpc.utils.OcpcUtils.getIdeaUpdates
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory


object OcpcCPCbid {
  def main(args: Array[String]): Unit = {
    /*
    1. 抽取前72小时的展现数据
    2. 按照前72小时每个ideaid的展现数据，计算ideaid维度下的平均bid
    3. 将计算出的平均bid存储到pb文件中
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val rawData = getBaseData(date, hour, spark)

    val data = calculateBid(rawData, date, hour, spark)

    data.repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_check_cpc_bid20190119")
  }

  def calculateBid(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val result = rawData
      .groupBy("ideaid")
      .agg(avg(col("bid")).alias("bid"))

    result.show(10)
    result
  }

  def getBaseData(date: String, hour: String, spark: SparkSession) = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -2)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)
    println(selectCondition)

    // 抽取数据
    val rawData = spark
      .table("dl_cpc.filtered_union_log_hourly")
      .where(selectCondition)
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    rawData.createOrReplaceTempView("raw_data")

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  bid as original_bid,
         |  (case when length(ocpc_log)>0 then cast(ocpc_log_dict['dynamicbid'] as int)
         |        else bid end) as bid,
         |  ocpc_log
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest)

    result.show(10)

    result
  }
}
