package com.cpc.spark.OcpcProtoType.report_novel

import com.cpc.spark.tools.{testOperateMySQL, OperateMySQL}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcUnionReportV3 {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession.builder().appName("OcpcUnionAucReport").enableHiveSupport().getOrCreate()
    // get the unit data
    val dataUnitRaw = unionDetailReport("_unitid", date, hour, spark)
    val dataUnit = addSuggestCPA(dataUnitRaw, "_unitid", date, hour, spark)
//    dataUnit.write.mode("overwrite").saveAsTable("test.ocpc_check_data20190422a")
    // get the user data
    val dataUserRaw = unionDetailReport("_userid", date, hour, spark)
    val dataUser = addSuggestCPA(dataUserRaw, "_userid", date, hour, spark)
//    dataUser.write.mode("overwrite").saveAsTable("test.ocpc_check_data20190422b")

    println("------union detail report success---------")
    val dataConversion = unionSummaryReport(date, hour, spark)
    println("------union summary report success---------")
    saveDataToMysql(dataUnit, dataUser, dataConversion, date, hour, spark)
    println("------insert into mysql success----------")
  }

  def addSuggestCPA(rawData: DataFrame, versionPostfix: String, date: String, hour: String, spark: SparkSession) = {
    val version1 = "novel_v2" + versionPostfix
//    val version2 = "qtt_hidden" + versionPostfix
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  conversion_goal,
         |  version,
         |  cpa_suggest,
         |  cali_value,
         |  cali_pcvr,
         |  cali_postcvr,
         |  smooth_factor,
         |  hourly_expcvr,
         |  hourly_calivalue,
         |  hourly_calipcvr,
         |  hourly_calipostcvr
         |FROM
         |  dl_cpc.ocpc_cali_detail_report_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version in ('$version1')
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    // 数据关联
    val result = rawData
      .join(data, Seq("identifier", "conversion_goal", "version"), "left_outer")
      .na.fill(0.0, Seq("cpa_suggest"))

    result
  }

  def unionDetailReport(versionPostfix: String, date: String, hour: String, spark: SparkSession): DataFrame ={
    val version1 = "novel_v2" + versionPostfix
//    val version2 = "novel_v1" + versionPostfix
    val sql =
      s"""
         |select
         |    identifier,
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
         |    round(cost*10.0/impression,3) as cpm,
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
         |    version = '$version1'
       """.stripMargin
    println(sql)
    val dataDF = spark.sql(sql)
    dataDF

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
        |    0 as is_hidden,
        |    cpa_given,
        |    cpa_real,
        |    cpa_ratio
        |from
        |    dl_cpc.ocpc_summary_report_hourly_v4
        |where
        |    `date` = '$date'
        |and
        |    hour = '$hour'
        |and
        |    version = 'novel_v2'
      """.stripMargin
    val dataDF = spark.sql(sql)
    dataDF

  }


  def saveDataToMysql(dataUnit: DataFrame, dataUser: DataFrame, dataConversion: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val hourInt = hour.toInt
    // unitid详情表
    val dataUnitMysql = dataUnit
      .withColumn("unit_id", col("identifier"))
      .select("user_id", "unit_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "cpm", "is_hidden", "cali_value", "cali_pcvr", "cali_postcvr", "smooth_factor", "cpa_suggest", "hourly_expcvr", "hourly_calivalue", "hourly_calipcvr", "hourly_calipostcvr")
      .na.fill(0, Seq("step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "cpm", "cali_value", "cali_pcvr", "cali_postcvr", "smooth_factor", "cpa_suggest", "hourly_expcvr", "hourly_calivalue", "hourly_calipcvr", "hourly_calipostcvr"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hourInt))
    val reportTableUnit = "report2.report_ocpc_data_detail_novel_v2"
    val delSQLunit = s"delete from $reportTableUnit where `date` = '$date' and hour = $hourInt"

    OperateMySQL.update(delSQLunit) //先删除历史数据
    OperateMySQL.insert(dataUnitMysql, reportTableUnit) //插入数据


    // userid详情表
    val dataUserMysql = dataUser
      .select("user_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "cpm", "is_hidden", "cali_value", "cali_pcvr", "cali_postcvr", "smooth_factor", "cpa_suggest", "hourly_expcvr", "hourly_calivalue", "hourly_calipcvr", "hourly_calipostcvr")
      .na.fill(0, Seq("step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "cpm", "cali_value", "cali_pcvr", "cali_postcvr", "smooth_factor", "cpa_suggest", "hourly_expcvr", "hourly_calivalue", "hourly_calipcvr", "hourly_calipostcvr"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hourInt))
    val reportTableUser = "report2.report_ocpc_data_user_detail_novel"
    val delSQLuser = s"delete from $reportTableUser where `date` = '$date' and hour = $hourInt"

    OperateMySQL.update(delSQLuser) //先删除历史数据
    OperateMySQL.insert(dataUserMysql, reportTableUser) //插入数据

    // 汇总表
    val dataConversionMysql = dataConversion
      .select("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "is_hidden", "cpa_given", "cpa_real", "cpa_ratio")
      .na.fill(0, Seq("total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "cpa_given", "cpa_real", "cpa_ratio"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hourInt))
    val reportTableConversion = "report2.report_ocpc_data_summary_novel_v2"
    val delSQLconversion = s"delete from $reportTableConversion where `date` = '$date' and hour = $hourInt"

    OperateMySQL.update(delSQLconversion) //先删除历史数据
    OperateMySQL.insert(dataConversionMysql, reportTableConversion) //插入数据
  }

}
