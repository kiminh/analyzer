package com.cpc.spark.OcpcProtoType.aa_ab_report

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object IndustryAaReportHourly {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(0).toString
    val spark = SparkSession.builder().appName("IndustryAaReportHourly").enableHiveSupport().getOrCreate()
    GetBaseData.getBaseData(date, hour, spark)
    println("------has got base data-------")
    getIndexValue(date, hour, spark)
    println("------has got index value-------")
  }

  def getIndexValue(date: String, hour: String, spark: SparkSession): Unit ={
    // 首先统计每天的分行业的user数和unit数
    val sql1 =
      s"""
        |select
        |    industry,
        |    count(userid) as ocpc_user_num,
        |    count(unitid) as ocpc_unit_num,
        |    count(case when is_hidden = 1 then 1 else 0 end) as ocpc_hidden_num
        |from
        |    (select
        |        industry,
        |        userid,
        |        unitid,
        |        is_hidden
        |    from
        |        dl_cpc.ocpc_aa_ab_report_base_data
        |    where
        |        `date` = '2019-03-30'
        |    and
        |        hour = '10'
        |    and
        |        version = 'qtt_demo'
        |    group by
        |        industry,
        |        userid,
        |        unitid,
        |        is_hidden) temp
        |group by
        |    industry
      """.stripMargin

    // 获取明投暗投控制数
    val sql2 =
      s"""
        |select
        |    industry,
        |    count(case when cpareal < cpagiven * 1.2 then 1 else 0 end) as cpa_control_num,
        |    count(case when hidden_cpareal < hidden_cpagiven * 1.2 then 1 else 0 end) as hidden_control_num,
        |    count(case when hidden_cost >= hidden_budget then 1 else 0 end) as hit_line_num,
        |    avg(hidden_cost) as avg_hidden_cost,
        |    avg(hidden_budget) as avg_hidden_budget
        |from(select
        |        industry,
        |        unitid,
        |        userid,
        |        sum(case when isclick = 1 then cpagiven else 0 end) * 0.01 / sum(isclick) as cpagiven,
        |        sum(case when isclick = 1 then price else 0 end) * 0.01 / sum(iscvr) as cpareal,
        |        sum(case when isclick = 1 and is_hidden = 1 then cpagiven else 0 end) * 0.01
        |        / sum(case when isclick = 1 and is_hidden = 1 then 1 else 0 end) as hidden_cpagiven,
        |        sum(case when isclick = 1 and is_hidden = 1 then price else 0 end) * 0.01
        |        / sum(case when isclick = 1 and is_hidden = 1 then iscvr else 0 end) as hidden_cpareal,
        |        sum(case when isclick = 1 and is_hidden = 1 then price else 0 end) * 0.01 as hidden_cost,
        |        max(case when isclick = 1 then budget * 0.01 else 0 end) as hidden_budget
        |    from
        |        dl_cpc.ocpc_aa_ab_report_base_data
        |    where
        |        `date` = '$date'
        |    and
        |        hour = '$hour'
        |    and
        |        version = 'qtt_demo'
        |    group by
        |        industry,
        |        unitid,
        |        userid) temp
        |group by
        |    industry
      """.stripMargin

    // 从行业整体统计字段
    val sql3 =
      s"""
        |select
        |    industry,
        |    sum(iscvr) as cv
        |    sum(isclick) as click,
        |    sum(isshow) as show,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as cost,
        |    round(sum(case when isclick = 1 then price else 0 end) * 10.0 / sum(isshow), 4) as cpm,
        |    sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 0.01 as ocpc_cost,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end)
        |        / sum(case when isclick = 1 then price else 0 end), 4) as ocpc_cost_ratio,
        |    sum(case when isclick = 1 and is_hidden = 1 then price else 0 end) * 0.01 as ocpc_hidden_cost,
        |    round(sum(case when isclick = 1 and is_hidden = 1 then price else 0 end)
        |        / sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end), 4) as ocpc_hidden_cost_ratio,
        |    round(c.hidden_control_num * 1.0 / b.ocpc_hidden_num, 4) as hidden_control_ratio,
        |from
        |    dl_cpc.ocpc_aa_ab_report_base_data
        |where
        |    `date` = '$date'
        |and
        |    hour = '$hour'
        |and
        |    version = 'qtt_demo'
        |group by
        |    industry
      """.stripMargin

    println("--------get index sql1--------")
    println(sql1)
    spark.sql(sql1).createOrReplaceTempView("unit_user_num_table")

    println("--------get index sql2--------")
    println(sql2)
    spark.sql(sql2).createOrReplaceTempView("control_table")

    println("--------get index sql3--------")
    println(sql3)
    spark.sql(sql3).createOrReplaceTempView("other_index_table")

    // 统计所有指标值
    val sql4 =
      s"""
        |select
        |    a.industry,
        |    b.ocpc_user_num,
        |    b.ocpc_unit_num,
        |    a.cv,
        |    a.click,
        |    a.show,
        |    a.cost,
        |    a.cpm,
        |    a.ocpc_cost,
        |    a.ocpc_cost_ratio,
        |    c.cpa_control_num,
        |    round(c.cpa_control_num * 1.0/ b.ocpc_unit_num, 4) as cpa_control_ratio,
        |    b.ocpc_hidden_num,
        |    a.ocpc_hidden_cost,
        |    a.ocpc_hidden_cost_ratio,
        |    c.hidden_control_num,
        |    round(c.hidden_control_num * 1.0 / b.ocpc_hidden_num, 4) as hidden_control_ratio,
        |    c.hit_line_num,
        |    round(c.hit_line_num * 1.0 / b.ocpc_hidden_num, 4) as hidden_hit_line_ratio,
        |    c.avg_hidden_cost,
        |    c.avg_hidden_budget,
        |    round(c.avg_hidden_cost * 1.0 / c.avg_hidden_budget) as hidden_budget_cost_ratio
        |from
        |    other_index_table a
        |left join
        |    unit_user_num_table b
        |on
        |    a.industry = b.industry
        |left join
        |    control_table c
        |on
        |    a.industry = c.industry
      """.stripMargin
    println("--------get index sql4--------")
    println(sql4)
    val dataDF = spark.sql(sql4)
    dataDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
      .repartition(50)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_industry_aa_report_hourly")
  }
}
