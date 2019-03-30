package com.cpc.spark.OcpcProtoType.aa_ab_report

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object UnitAaReportHourly {
  def main(args: Array[String]): Unit = {

  }

  // 首先获取基础数据
  def getIndexValue(date: String, hour: String, spark: SparkSession): Unit ={
    val sql1 =
      """
        |select
        |    unitid,
        |    userid,
        |    industry,
        |    round((case when sum(case when isclick is null then 0 else 1 end) = 0 then avg(is_ocpc)
        |                else sum(case when isclick = 1 then is_ocpc else 0 end) / sum(case when isclick is null then 0 else 1 end) end), 4) as put_type,
        |    adslot_type,
        |    conversion_goal,
        |    sum(iscvr) as cv,
        |    sum(isclick) as click,
        |    sum(isshow) as show,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as cost,
        |    round(sum(case when isclick = 1 then price else 0 end) * 10.0 / sum(isshow), 4) as cpm,
        |    sum(case when isclick = 1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as exp_cvr,
        |    round(sum(case when isclick = 1 then pcvr else 0 end) * 1.0 / sum(isclick), 4) as pre_cvr,
        |    round(sum(iscvr) * 1.0 / sum(isclick), 4) as post_cvr,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(isclick), 4) as cost_of_every_click,
        |    round(sum(case when isclick = 1 then bid else 0 end) * 0.01/ sum(isclick), 4) as bid_of_every_click,
        |    round(sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(iscvr), 4) as cpa_real,
        |    round(sum(case when isclick = 1 then cpa_given else 0 end) / sum(isclick) * 0.01, 4)  as cpa_given,
        |    round(sum(case when isclick = 1 and is_hidden = 1 then price else 0 end)
        |          / sum(case when isclick = 1 then price else 0 end), 4) hidden_cost_ratio,
        |    round(sum(case when isclick = 1 then kvalue else 0 end) / sum(isclick), 4) as kvalue,
        |    max(case when isclick = 1 then budget * 0.01 else 0 end) as budget,
        |    round(case when (max(case when isclick = 1 then budget * 0.01 else 0 end)) = 0 then 0
        |               else (sum(case when isclick = 1 then price else 0 end) / max(case when isclick = 1 then budget * 0.01 else 0 end)) end, 4) as cost_budget_ratio,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 10.0
        |          / sum(case when isshow= 1 and is_ocpc = 1 then 1 else 0 end), 4) as ocpc_cpm,
        |    round(sum(case when isclick = 1 and is_ocpc != 1 then price else 0 end) * 10.0
        |          / sum(case when isshow= 1 and is_ocpc != 1 then 1 else 0 end), 4) as cpc_cpm,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then pcvr else 0 end) * 1.0
        |         / sum(case when isclick = 1 and is_ocpc = 1 then 1 else 0 end), 4) as ocpc_pre_cvr,
        |    round(sum(case when isclick = 1 and is_ocpc != 1 then pcvr else 0 end) * 1.0
        |         / sum(case when isclick = 1 and is_ocpc != 1 then 1 else 0 end), 4) as cpc_pre_cvr,
        |    round(sum(case when is_ocpc = 1 then iscvr else 0 end) * 1.0
        |         / sum(case when isclick = 1 and is_ocpc = 1 then 1 else 0 end), 4) as ocpc_post_cvr,
        |    round(sum(case when is_ocpc != 1 then iscvr else 0 end) * 1.0
        |         / sum(case when is_ocpc != 1 and isclick = 1 then 1 else 0 end), 4) as cpc_post_cvr,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 0.01
        |         / sum(case when isclick = 1 and is_ocpc = 1 then 1 else 0 end), 4) as ocpc_cost_of_every_click,
        |    round(sum(case when isclick = 1 and is_ocpc != 1 then price else 0 end) * 0.01
        |         / sum(case when isclick = 1 and is_ocpc != 1 then 1 else 0 end), 4) as cpc_cost_of_every_click,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then bid else 0 end) * 0.01
        |         / sum(case when isclick = 1 and is_ocpc = 1 then 1 else 0 end), 4) as ocpc_bid_of_every_click,
        |    round(sum(case when isclick = 1 and is_ocpc != 1 then bid else 0 end) * 0.01
        |         / sum(case when isclick = 1 and is_ocpc != 1 then 1 else 0 end), 4) as cpc_bid_of_every_click,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 0.01
        |         / sum(case when is_ocpc = 1 then iscvr else 0 end), 4) as ocpc_cpa_real,
        |    round(sum(case when isclick = 1 and is_ocpc != 1 then price else 0 end) * 0.01
        |         / sum(case when is_ocpc != 1 then iscvr else 0 end), 4) as cpc_cpa_real
        |from
        |    dl_cpc.ocpc_aa_ab_report_base_data
        |group by
        |    unitid,
        |    userid,
        |    industry,
        |    adslot_type,
        |    conversion_goal
      """.stripMargin
    println("-----sql1------")
    println(sql1)
    spark.sql(sql1).createOrReplaceTempView("temp_table")

    val sql2 =
      s"""
        |select
        |    unitid,
        |    userid,
        |    industry,
        |    put_type,
        |    adslot_type,
        |    conversion_goal,
        |    cv,
        |    click,
        |    show,
        |    cost,
        |    cpm,
        |    exp_cvr,
        |    pre_cvr,
        |    post_cvr,
        |    cost_of_every_click,
        |    bid_of_every_click,
        |    cpa_real,
        |    cpa_given,
        |    hidden_cost_ratio,
        |    kvalue,
        |    budget,
        |    cost_budget_ratio,
        |    round((case when cpc_cpm != 0 then ocpc_cpm / cpc_cpm else 0 end), 4) as ocpc_cpc_cpm_ratio,
        |    round((case when cpc_pre_cvr != 0 then ocpc_pre_cvr / cpc_pre_cvr else 0 end), 4) as ocpc_cpc_pre_cvr_ratio,
        |    round((case when cpc_post_cvr != 0 then ocpc_post_cvr / cpc_post_cvr else 0 end), 4) as ocpc_cpc_post_cvr_ratio,
        |    round((case when cpc_cost_of_every_click != 0 then ocpc_cost_of_every_click / cpc_cost_of_every_click else 0 end), 4) as ocpc_cpc_cost_of_every_click,
        |    round((case when cpc_bid_of_every_click != 0 then ocpc_bid_of_every_click / cpc_bid_of_every_click else 0 end), 4) as ocpc_cpc_bid_of_every_click,
        |    round((case when cpc_cpa_real != 0 then ocpc_cpa_real / cpc_cpa_real else 0 end), 4) as ocpc_cpc_cpa_real
        |from
        |    temp_table
      """.stripMargin
    println("-----sql2------")
    println(sql2)
    val aaReportDF = spark.sql(sql2)
    aaReportDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
      .repartition(50)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_aa_report_hourly")

  }
}
