package com.cpc.spark.OcpcProtoType.report

import com.cpc.spark.tools.OperateMySQL
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcUnionReport {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession.builder().appName("OcpcUnionAucReport").enableHiveSupport().getOrCreate()
    // get the unit data
    val dataUnitRaw = unionDetailReport(date, hour, spark)
    // get the suggest cpa
    val dataUnit = addSuggestCPA(dataUnitRaw, date, hour, spark)
    dataUnit.write.mode("overwrite").saveAsTable("test.check_ocpc_data20190413a")
//    println("------union detail report success---------")
//    val dataConversion = unionSummaryReport(date, hour, spark)
//    println("------union summary report success---------")
//    saveDataToMysql(dataUnit, dataConversion, date, hour, spark)
//    println("------insert into mysql success----------")
  }

  def addSuggestCPA(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  identifier as unit_id,
         |  conversion_goal,
         |  version,
         |  cpa_suggest
         |FROM
         |  dl_cpc.ocpc_cali_detail_report_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version in ('qtt_demo', 'qtt_hidden')
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    // 数据关联
    val result = rawData
      .join(data, Seq("unit_id", "conversion_goal", "version"), "left_outer")
      .na.fill(0.0, Seq("cpa_suggest"))

    result
  }

  def unionDetailReport(date: String, hour: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
        |select
        |    identifier as unit_id,
        |    userid as user_id,
        |    conversion_goal,
        |    step2_click_percent,
        |    is_step2,
        |    cpa_given,
        |    cpa_real,
        |    cpa_ratio,
        |    is_cpa_ok,
        |    impression,
        |    click,
        |    conversion,
        |    ctr,
        |    click_cvr,
        |    show_cvr,
        |    cost,
        |    acp,
        |    avg_k,
        |    recent_k,
        |    pre_cvr,
        |    post_cvr,
        |    q_factor,
        |    acb,
        |    auc,
        |    hour,
        |    version,
        |    0 as is_hidden
        |from
        |    dl_cpc.ocpc_detail_report_hourly_v4
        |where
        |    `date` = '$date'
        |and
        |    hour = '$hour'
        |and
        |    version = 'qtt_demo'
        |
        |union
        |
        |select
        |    identifier as unit_id,
        |    userid as user_id,
        |    conversion_goal,
        |    step2_click_percent,
        |    is_step2,
        |    cpa_given,
        |    cpa_real,
        |    cpa_ratio,
        |    is_cpa_ok,
        |    impression,
        |    click,
        |    conversion,
        |    ctr,
        |    click_cvr,
        |    show_cvr,
        |    cost,
        |    acp,
        |    avg_k,
        |    recent_k,
        |    pre_cvr,
        |    post_cvr,
        |    q_factor,
        |    acb,
        |    auc,
        |    hour,
        |    version,
        |    1 as is_hidden
        |from
        |    dl_cpc.ocpc_detail_report_hourly_v4
        |where
        |    `date` = '$date'
        |and
        |    hour = '$hour'
        |and
        |    version = 'qtt_hidden'
      """.stripMargin
    val dataDF = spark.sql(sql)
    dataDF
//      .withColumn("date", lit(date))
//      .repartition(10)
//      .write.mode("overwrite").insertInto("test.wt_union_detail_report")
  }

  def unionSummaryReport(date: String, hour: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
        |select
        |    conversion_goal,
        |    total_adnum,
        |    step2_adnum,
        |    low_cpa_adnum,
        |    high_cpa_adnum,
        |    step2_cost,
        |    step2_cpa_high_cost,
        |    impression,
        |    click,
        |    conversion,
        |    ctr,
        |    click_cvr,
        |    cost,
        |    acp,
        |    pre_cvr,
        |    post_cvr,
        |    q_factor,
        |    acb,
        |    auc,
        |    hour,
        |    version,
        |    0 as is_hidden
        |from
        |    dl_cpc.ocpc_summary_report_hourly_v4
        |where
        |    `date` = '$date'
        |and
        |    hour = '$hour'
        |and
        |    version = 'qtt_demo'
        |
        |union
        |
        |select
        |    conversion_goal,
        |    total_adnum,
        |    step2_adnum,
        |    low_cpa_adnum,
        |    high_cpa_adnum,
        |    step2_cost,
        |    step2_cpa_high_cost,
        |    impression,
        |    click,
        |    conversion,
        |    ctr,
        |    click_cvr,
        |    cost,
        |    acp,
        |    pre_cvr,
        |    post_cvr,
        |    q_factor,
        |    acb,
        |    auc,
        |    hour,
        |    version,
        |    1 as is_hidden
        |from
        |    dl_cpc.ocpc_summary_report_hourly_v4
        |where
        |    `date` = '$date'
        |and
        |    hour = '$hour'
        |and
        |    version = 'qtt_hidden'
      """.stripMargin
    val dataDF = spark.sql(sql)
    dataDF
//      .withColumn("date", lit(date))
//      .repartition(10)
//      .write.mode("overwrite").insertInto("test.wt_union_summary_report")
  }


  def saveDataToMysql(dataUnit: DataFrame, dataConversion: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val hourInt = hour.toInt
    // 详情表
    val dataUnitMysql = dataUnit
      .select("user_id", "unit_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "is_hidden", "cpa_suggest")
      .na.fill(0, Seq("step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hourInt))
    val reportTableUnit = "report2.report_ocpc_data_detail_v2"
    val delSQLunit = s"delete from $reportTableUnit where `date` = '$date' and hour = $hourInt"

    OperateMySQL.update(delSQLunit) //先删除历史数据
    OperateMySQL.insert(dataUnitMysql, reportTableUnit) //插入数据

    // 汇总表
    val dataConversionMysql = dataConversion
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "is_hidden")
      .na.fill(0, Seq("total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp", "pre_cvr", "post_cvr", "q_factor", "acb", "auc"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hourInt))
    val reportTableConversion = "report2.report_ocpc_data_summary_v2"
    val delSQLconversion = s"delete from $reportTableConversion where `date` = '$date' and hour = $hourInt"

    OperateMySQL.update(delSQLconversion) //先删除历史数据
    OperateMySQL.insert(dataConversionMysql, reportTableConversion) //插入数据
  }

}
