package com.cpc.spark.ocpc

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcHourlyReportV2 {
  def main(args: Array[String]): Unit = {
    /*

     */
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

//    getHourlyReport(date, hour, spark)
  }

  def getHourlyReport(date: String, hour: String, spark: SparkSession) = {
    /**
      * 重新计算抽取全天截止当前时间的数据日志
      */

    // 抽取基础数据：所有跑ocpc的广告主
    val rawData = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(s"`dt`='$date' and `hour` <= '$hour'")
      .withColumn("bid_ocpc", col("cpa_given"))
      .filter(s"length(ocpc_log)>0")
//    rawData.write.mode("overwrite").saveAsTable("test.ocpc_hourly_complete_data")


//    noApiData
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_check_hourly_report_noapi")
//    apiData
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_check_hourly_report_api")
  }


}