package com.cpc.spark.ocpc.report

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcDataDetail {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    exportHourlyReport(date, hour, spark)
  }

  def exportHourlyReport(date: String, hour: String, spark: SparkSession) = {
    // 读取数据
    val apiData = spark
      .table("dl_cpc.ocpc_check_hourly_report_api")
      .where(s"`date`='$date' and `hour`='$hour'")
      .withColumn("conversion_goal", lit(2))
      .select("ideaid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "price", "avg_k", "recent_k")

    val noApiData = spark
      .table("dl_cpc.ocpc_check_hourly_report_noapi")
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("ideaid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "price", "avg_k", "recent_k")

    // 把两个部分数据连接到一起
    val rawData = apiData.union(noApiData)
    rawData.show(10)

    val count1 = apiData.count()
    val count2 = noApiData.count()
    val count3 = rawData.count()
    println(s"count of apiData is $count1, count of noApiData is $count2, count of total dataset is $count3")
  }
}