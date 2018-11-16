package com.cpc.spark.ocpc.report

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcDataSummary {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString

    exportDailyReport(date, spark)
  }

  def exportDailyReport(date: String, spark: SparkSession) = {
    // 读取数据
    val apiData = spark
      .table("dl_cpc.ocpc_check_daily_report_api")
      .where(s"`date`='$date'")
      .withColumn("conversion_goal", lit(2))
      .select("ideaid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "price", "avg_k")

    val noApiData = spark
      .table("dl_cpc.ocpc_check_daily_report_noapi")
      .where(s"`date`='$date'")
      .select("ideaid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "price", "avg_k")

    // 把两个部分数据连接到一起
    val rawData = apiData.union(noApiData)
    rawData.show(10)

    // 计算其他相关特征
    val data = rawData
      .withColumn("is_step2", when(col("step2_percent")===1, 1).otherwise(0))
      .withColumn("cpa_ratio", col("cpa_given") * 1.0 / col("cpa_real"))
      .withColumn("is_low_cpa", when(col("is_step2")===1 && (col("cpa_ratio")<0.8 || col("cpa_real").isNull), 1).otherwise(0))
      .withColumn("is_high_cpa", when(col("is_step2")===1 && col("cpa_ratio")>=0.8, 1).otherwise(0))
      .withColumn("cost", col("price") * col("ctr_cnt"))
    data.printSchema()
    data.show(10)

    // summary data
    val resultDF = data
      .groupBy("conversion_goal")
      .agg(
        count(col("ideaid")).alias("total_adnum"),
        sum(col("is_step2")).alias("step2_adnum"),
        sum(col("is_low_cpa")).alias("low_cpa_adnum"),
        sum(col("is_high_cpa")).alias("high_cpa_adnum"),
        sum(col("show_cnt")).alias("impression"),
        sum(col("ctr_cnt")).alias("click"),
        sum(col("cvr_cnt")).alias("conversion"),
        sum(col("cost")).alias("cost")
      )
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "impression", "click", "conversion", "cost")
      .withColumn("ctr", col("click") * 1.0 / col("impression"))
      .withColumn("click_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("acp", col("cost") * 1.0 / col("click"))
      .withColumn("date", lit(date))
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp", "date")

    resultDF.printSchema()
    resultDF.show(10)

  }
}