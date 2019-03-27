package com.cpc.spark.OcpcProtoType.report_qtt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcUnionReport {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession.builder().appName("OcpcUnionAucReport").enableHiveSupport().getOrCreate()
    unionAucReport(date, hour, spark)
    println("------success---------")
  }

  def unionAucReport(date: String, hour: String, spark: SparkSession): Unit ={
    val sql =
      s"""
        |select
        |    identifier,
        |    userid,
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
        |    version
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
        |    identifier,
        |    userid,
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
        |    version
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
      .withColumn("date", lit(date))
      .repartition(10)
      .write.mode("overwrite").insertInto("test.wt_union_auc_report")
  }
}
