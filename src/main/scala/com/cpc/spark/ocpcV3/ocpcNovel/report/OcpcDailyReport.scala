package com.cpc.spark.ocpcV3.ocpcNovel.report

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcDailyReport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val summaryReport = getHourlyReport(date, hour, spark)
    val tableName = "dl_cpc.ocpcv3_novel_report_summary_hourly"
//    summaryReport.write.mode("overwrite").saveAsTable(tableName)
    summaryReport.write.mode("overwrite").insertInto(tableName)
    println(s"successfully save table into $tableName")
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
         |    SUM(case when is_step2=1 and is_cpa_ok=0 then cost else 0 end) as step2_cpa_high_cost,
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
         |    SUM(case when is_step2=1 and is_cpa_ok=0 then cost else 0 end) as step2_cpa_high_cost,
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
       """.stripMargin
    println(sqlRequest2)
    val rawData2 = spark.sql(sqlRequest2)
    val rawData = rawData2.union(rawData1)

    // 计算其他数据
//    ctr
//    click_cvr
//    acp
    val data = rawData
      .withColumn("ctr", col("click") * 1.0 / col("impression"))
      .withColumn("click_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("acp", col("cost") * 1.0 / col("click"))

    data.show(10)

    val resultDF = data
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF

  }
}