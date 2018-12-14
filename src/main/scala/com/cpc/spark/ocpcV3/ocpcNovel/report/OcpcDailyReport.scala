package com.cpc.spark.ocpcV3.ocpcNovel.report

import java.util.Properties
import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.{DataFrame, SparkSession, SaveMode}
import org.apache.spark.sql.functions._

object OcpcDailyReport {
  var mariadb_write_url = ""
  val mariadb_write_prop = new Properties()
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val summaryReport = getHourlyReport(date, hour, spark)
    val tableName = "test.ocpcv3_novel_report_summary_hourly"
//    summaryReport.write.mode("overwrite").insertInto(tableName)
    summaryReport.write.mode("overwrite").saveAsTable(tableName)
    println(s"successfully save table into $tableName")
//    saveDataToReport(summaryReport, spark)
  }

  def getHourlyReport(date: String, hour: String, spark: SparkSession) = {
    val tableName = "dl_cpc.ocpcv3_novel_report_detail_hourly"

    // 基础统计
    val sqlRequest1 =
      s"""
         |SELECT
         |    conversion_goal,
         |    COUNT(1) as total_adnum,
         |    SUM(case when is_step2=1 then 1 else 0 end) as step2_adnum,
         |    SUM(case when is_cpa_ok=1 and is_step2=1 then 1 else 0 end) as low_cpa_adnum,
         |    SUM(case when is_cpa_ok=0 and is_step2=1 then 1 else 0 end) as high_cpa_adnum,
         |    SUM(case when is_step2=1 then cost else 0 end) as step2_cost,
         |    SUM(case when is_step2=1 and is_cpa_ok=0 then (cost - cpa_given * conversion * 2) else 0 end) as step2_cpa_high_cost,
         |    SUM(impression) as impression,
         |    SUM(click) as click,
         |    SUM(conversion) as conversion,
         |    SUM(cost) as cost
         |FROM
         |    $tableName
         |WHERE
         |    `date`='$date'
         |AND
         |    `hour`='$hour'
         |AND
         |    conversion_goal is not null
         |GROUP BY conversion_goal
       """.stripMargin
    println(sqlRequest1)
    val rawData1 = spark.sql(sqlRequest1)
    val sqlRequest2 =
      s"""
         |SELECT
         |    0 as conversion_goal,
         |    COUNT(1) as total_adnum,
         |    SUM(case when is_step2=1 then 1 else 0 end) as step2_adnum,
         |    SUM(case when is_cpa_ok=1 and is_step2=1 then 1 else 0 end) as low_cpa_adnum,
         |    SUM(case when is_cpa_ok=0 and is_step2=1 then 1 else 0 end) as high_cpa_adnum,
         |    SUM(case when is_step2=1 then cost else 0 end) as step2_cost,
         |    SUM(case when is_step2=1 and is_cpa_ok=0 then (cost - cpa_given * conversion * 2) else 0 end) as step2_cpa_high_cost,
         |    SUM(impression) as impression,
         |    SUM(click) as click,
         |    SUM(conversion) as conversion,
         |    SUM(cost) as cost
         |FROM
         |    $tableName
         |WHERE
         |    `date`='$date'
         |AND
         |    `hour`='$hour'
         |AND
         |    conversion_goal is not null
       """.stripMargin
    println(sqlRequest2)
    val rawData2 = spark.sql(sqlRequest2)
    val rawData = rawData2
      .union(rawData1)
      .na.fill(0, Seq("low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "cost"))

    // 计算其他数据
//    ctr
//    click_cvr
//    acp
    val data = rawData
      .withColumn("ctr", col("click") * 1.0 / col("impression"))
      .withColumn("click_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("click_cvr", when(col("click")===0, 1).otherwise(col("click_cvr")))
      .withColumn("acp", col("cost") * 1.0 / col("click"))
      .withColumn("acp", when(col("click")===0, 0).otherwise(col("acp")))

    data.show(10)

    val resultDF = data
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF

  }

  def saveDataToReport(data: DataFrame, spark: SparkSession) = {
    val conf = ConfigFactory.load()
    val tableName = "report2.report_ocpc_novel_data_summary"
    mariadb_write_url = conf.getString("mariadb.report2_write.url")
    mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
    mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
    mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

    println("#################################")
    println("count:" + data.count())
    println("url: " + conf.getString("mariadb.report2_write.url"))
    println("table name: " + tableName)
    println("user: " + conf.getString("mariadb.report2_write.user"))
    println("password: " + conf.getString("mariadb.report2_write.password"))
    println("driver: " + conf.getString("mariadb.report2_write.driver"))
    data.show(10)

    data
      .write
      .mode(SaveMode.Append)
      .jdbc(mariadb_write_url, tableName, mariadb_write_prop)

  }
}