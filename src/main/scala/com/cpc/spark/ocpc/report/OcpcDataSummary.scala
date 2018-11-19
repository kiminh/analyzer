package com.cpc.spark.ocpc.report

import java.util.Properties
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object OcpcDataSummary {
  var mariadb_write_url = ""
  val mariadb_write_prop = new Properties()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString

    val data = exportDailyReport(date, spark)
    saveDataSummaryToReport(data, spark)
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
      .withColumn("is_high_cpa", when(col("is_step2")===1 && (col("cpa_ratio")<0.8 || col("cpa_real").isNull), 1).otherwise(0))
      .withColumn("is_low_cpa", when(col("is_step2")===1 && col("cpa_ratio")>=0.8, 1).otherwise(0))
      .withColumn("cost", col("price") * col("ctr_cnt"))

    // summary data
    val result = data
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

    val summaryData = result.agg(
      sum(col("total_adnum")).alias("total_adnum"),
      sum(col("step2_adnum")).alias("step2_adnum"),
      sum(col("low_cpa_adnum")).alias("low_cpa_adnum"),
      sum(col("high_cpa_adnum")).alias("high_cpa_adnum"),
      sum(col("impression")).alias("impression"),
      sum(col("click")).alias("click"),
      sum(col("conversion")).alias("conversion"),
      sum(col("cost")).alias("cost"))
      .withColumn("conversion_goal", lit(0))
      .withColumn("ctr", col("click")*1.0 / col("impression"))
      .withColumn("click_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("acp", col("cost") * 1.0 / col("click"))
      .withColumn("date", lit(date))
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp", "date")

    val resultDF = result.union(summaryData)

    resultDF.printSchema()
    resultDF

  }

  def saveDataSummaryToReport(data: DataFrame, spark: SparkSession) = {
    val conf = ConfigFactory.load()
    val tableName = "report2.report_ocpc_data_summary"
    mariadb_write_url = conf.getString("mariadb.report2_write.url")
    mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
    mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
    mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

    println("#################################")
    println("count:" + data.count())
    println("url: " + conf.getString("mariadb.report2_write.url"))
    println("table name: " + tableName)
//    println("user: " + conf.getString("mariadb.report2_write.user"))
//    println("password: " + conf.getString("mariadb.report2_write.password"))
//    println("driver: " + conf.getString("mariadb.report2_write.driver"))
    data.show(10)

//    data
//      .write
//      .mode(SaveMode.Append)
//      .jdbc(mariadb_write_url, tableName, mariadb_write_prop)

  }
}