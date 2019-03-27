package com.cpc.spark.OcpcProtoType.report_qtt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcUnionAucReport {
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
        |    indentifier,
        |    userid,
        |    conversion_goal,
        |    pre_cvr,
        |    post_cvr,
        |    q_factor,
        |    cpagiven,
        |    cpareal,
        |    acp,
        |    acb,
        |    auc,
        |    hour,
        |    version
        |from
        |    dl_cpc.ocpc_auc_report_detail_hourly
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
        |    indentifier,
        |    userid,
        |    conversion_goal,
        |    pre_cvr,
        |    post_cvr,
        |    q_factor,
        |    cpagiven,
        |    cpareal,
        |    acp,
        |    acb,
        |    auc,
        |    hour,
        |    version
        |from
        |    dl_cpc.ocpc_auc_report_detail_hourly
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
