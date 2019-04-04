package com.cpc.spark.OcpcProtoType.aa_ab_report

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object IndustryAbReportHourly {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession.builder().appName("IndustryAbReportHourly").enableHiveSupport().getOrCreate()
    getIndexValue(date, hour, spark)
    println("------has got all index--------")
  }

  def getIndexValue(date: String, hour: String, spark: SparkSession): Unit ={
    // 首先统计明投的信息
    val mingtouDF = getMingTouInfo(date, hour, spark)
    // 然后统计暗投的信息
    val antouDF = getAnTouInfo(date, hour, spark)
    // 统计总的信息
    val dataDF = getAllIndexValue(mingtouDF, antouDF, spark)
    println("------insert into hive--------")
    dataDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
      .repartition(50)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_industry_ab_report_hourly")
  }

  // 首先统计明投的信息
  def getMingTouInfo(date: String, hour: String, spark: SparkSession): DataFrame ={
    // 1、首先统计所有进行ab实验的unitid
    val sql1 =
      s"""
        |select
        |  unitid
        |from
        |  dl_cpc.ocpc_aa_ab_report_base_data
        |where
        |  is_ocpc = 1
        |and
        |  cast(ocpc_log_dict['cpcBid'] as int) > 0
        |and
        |  `date` = '$date'
        |and
        |  hour = '$hour'
        |and
        |  version = 'qtt_demo'
        |group by
        |  unitid
      """.stripMargin
    println("--------get ming tou info sql1--------")
    println(sql1)
    spark.sql(sql1).createOrReplaceTempView("minttou_ab_unit_table")

    // 2、计算每个unit的指标值
    val sql2 =
      s"""
        |select
        |  unitid,
        |  industry,
        |  type,
        |  unit_cv,
        |  unit_click,
        |  unit_show,
        |  unit_cost,
        |  (case when type = 'ocpc' then pcvr else exp_cvr end) as unit_cvr,
        |  unit_bid,
        |  unit_cpa_given,
        |  unit_uid_num
        |from
        |  (select
        |    unitid,
        |    industry,
        |    (case when exptags like '%cpcBid' and cast(ocpc_log_dict['cpcBid'] as int) > 0 then 'cpc'
        |          else 'ocpc' end) as type,
        |    sum(iscvr) as unit_cv,
        |    sum(isclick) as unit_click,
        |    sum(isshow) as unit_show,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as unit_cost,
        |    sum(case when isclick = 1 then pcvr else 0 end) as pcvr,
        |    sum(case when isclick = 1 then exp_cvr else 0 end) as exp_cvr,
        |    sum(case when isclick = 1 then bid else 0 end) as unit_bid,
        |    sum(case when isclick = 1 then cpa_given else 0 end) as unit_cpa_given,
        |    count(distinct uid) as unit_uid_num
        |  from
        |    dl_cpc.ocpc_aa_ab_report_base_data
        |  where
        |    is_ocpc = 1
        |  and
        |    `date` = '$date'
        |  and
        |    hour = '$hour'
        |  and
        |    version = 'qtt_demo'
        |  group by
        |    unitid,
        |    industry,
        |    (case when exptags like '%cpcBid' and cast(ocpc_log_dict['cpcBid'] as int) > 0 then 'cpc' else 'ocpc' end)) temp
      """.stripMargin
    println("--------get ming tou info sql2--------")
    println(sql2)
    spark.sql(sql2).createOrReplaceTempView("mingtou_unit_index_table")

    // 3、将前后两个表进行左关联
    val sql3 =
      s"""
        |select
        |  a.unitid,
        |  b.industry,
        |  b.type,
        |  b.unit_cv,
        |  b.unit_click,
        |  b.unit_show,
        |  b.unit_cost,
        |  b.unit_cvr,
        |  b.unit_bid,
        |  b.unit_cpa_given,
        |  b.unit_uid_num
        |from
        |  minttou_ab_unit_table a
        |left join
        |  mingtou_unit_index_table b
        |on
        |  a.unitid = b.unitid
      """.stripMargin
    println("--------get ming tou info sql3--------")
    println(sql3)
    val mingtouDF = spark.sql(sql3)
    mingtouDF
  }

  // 获得暗投信息
  def getAnTouInfo(date: String, hour: String, spark: SparkSession): DataFrame ={
    // 1、首先统计所有进行暗测的unitid
    val sql1 =
      s"""
        |select
        |  unitid
        |from
        |  dl_cpc.ocpc_aa_ab_report_base_data
        |where
        |  is_hidden = 1
        |and
        |  `date` = '$date'
        |and
        |  hour = '$hour'
        |and
        |  version = 'qtt_demo'
        |group by
        |  unitid
      """.stripMargin
    println("--------get an tou info sql1--------")
    println(sql1)
    spark.sql(sql1).createOrReplaceTempView("antou_ab_unit_table")

    // 2、计算每个unit的指标值
    val sql2 =
      s"""
        |select
        |  unitid,
        |  industry,
        |  type,
        |  unit_cv,
        |  unit_click,
        |  unit_show,
        |  unit_cost,
        |  (case when type = 'ocpc' then pcvr else exp_cvr end) as unit_cvr,
        |  unit_bid,
        |  unit_cpa_given,
        |  unit_uid_num
        |from
        |  (select
        |    unitid,
        |    industry,
        |    (case when is_ocpc = 0 then 'cpc' else 'ocpc' end) as type,
        |    sum(iscvr) as unit_cv,
        |    sum(isclick) as unit_click,
        |    sum(isshow) as unit_show,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as unit_cost,
        |    sum(case when isclick = 1 then pcvr else 0 end) as pcvr,
        |    sum(case when isclick = 1 then exp_cvr else 0 end) as exp_cvr,
        |    sum(case when isclick = 1 then bid else 0 end) as unit_bid,
        |    sum(case when isclick = 1 then cpa_given else 0 end) as unit_cpa_given,
        |    count(distinct uid) as unit_uid_num
        |  from
        |    dl_cpc.ocpc_aa_ab_report_base_data
        |  where
        |    `date` = '$date'
        |  and
        |    hour = '$hour'
        |  and
        |    version = 'qtt_demo'
        |  group by
        |    unitid,
        |    industry,
        |    (case when is_ocpc = 0 then 'cpc' else 'ocpc' end)) temp
      """.stripMargin
    println("--------get an tou info sql2--------")
    println(sql2)
    spark.sql(sql2).createOrReplaceTempView("antou_unit_index_table")

    // 3、将两个表进行左关联
    val sql3 =
      s"""
        |select
        |  a.unitid,
        |  b.industry,
        |  b.type,
        |  b.unit_cv,
        |  b.unit_click,
        |  b.unit_show,
        |  b.unit_cost,
        |  b.unit_cvr,
        |  b.unit_bid,
        |  b.unit_cpa_given,
        |  b.unit_uid_num
        |from
        |  antou_ab_unit_table a
        |left join
        |  antou_unit_index_table b
        |on
        |  a.unitid = b.unitid
      """.stripMargin
    println("--------get an tou info sql3--------")
    println(sql3)
    val antouDF = spark.sql(sql3)
    antouDF
  }

  // 统计总指标
  def getAllIndexValue(mingtouDF: DataFrame, antouDF: DataFrame, spark: SparkSession): DataFrame ={
    mingtouDF.createOrReplaceTempView("mingtou_index_table")
    antouDF.createOrReplaceTempView("antou_index_table")
    // 1、首先统计分行业的
    val sql1 =
      s"""
        |select
        |  industry,
        |  type,
        |  sum(unit_cv) as cv,
        |  sum(unit_click) as click,
        |  sum(unit_show) as show,
        |  sum(unit_cost) as cost,
        |  (case when sum(unit_show) > 0 then round(sum(unit_cost) * 1000.0 / sum(unit_show), 4)
        |        else 0 end) as cpm,
        |  (case when sum(unit_click) > 0 then round(sum(unit_cvr) * 1.0 / sum(unit_click), 4)
        |        else 0 end) as pre_cvr,
        |  (case when sum(unit_click) > 0 then round(sum(unit_cv) * 1.0 / sum(unit_click), 4)
        |        else 0 end) as post_cvr,
        |  (case when sum(unit_click) > 0 then round(sum(unit_cost) * 1.0 / sum(unit_click), 4)
        |        else 0 end) as cost_of_every_click,
        |  (case when sum(unit_click) > 0 then round(sum(unit_bid) * 1.0 / sum(unit_click), 4)
        |        else 0 end) as bid_of_every_click,
        |  (case when sum(unit_click) > 0 then round(sum(unit_cpa_given) * 1.0 / sum(unit_click), 4)
        |        else 0 end) as cpa_given,
        |  (case when sum(unit_cv) > 0 then round(sum(unit_cost) * 1.0 / sum(unit_cv), 4)
        |        else 0 end) as cpa_real,
        |  (case when sum(unit_uid_num) > 0 then round(sum(unit_cost) * 1000.0 / sum(unit_uid_num), 4)
        |        else 0 end) as arpu
        |from
        |  (select * from mingtou_index_table
        |  union
        |  select * from antou_index_table) temp
        |group by
        |  industry,
        |  type
      """.stripMargin
    println("--------get all index value sql1--------")
    println(sql1)
    spark.sql(sql1).createOrReplaceTempView("industry_index_table")

    // 2、然后统计整体的
    val sql2 =
      s"""
        |select
        |  'all' as industry,
        |  type,
        |  sum(unit_cv) as cv,
        |  sum(unit_click) as click,
        |  sum(unit_show) as show,
        |  sum(unit_cost) as cost,
        |  (case when sum(unit_show) > 0 then round(sum(unit_cost) * 1000.0 / sum(unit_show), 4)
        |        else 0 end) as cpm,
        |  (case when sum(unit_click) > 0 then round(sum(unit_cvr) * 1.0 / sum(unit_click), 4)
        |        else 0 end) as pre_cvr,
        |  (case when sum(unit_click) > 0 then round(sum(unit_cv) * 1.0 / sum(unit_click), 4)
        |        else 0 end) as post_cvr,
        |  (case when sum(unit_click) > 0 then round(sum(unit_cost) * 1.0 / sum(unit_click), 4)
        |        else 0 end) as cost_of_every_click,
        |  (case when sum(unit_click) > 0 then round(sum(unit_bid) * 1.0 / sum(unit_click), 4)
        |        else 0 end) as bid_of_every_click,
        |  (case when sum(unit_click) > 0 then round(sum(unit_cpa_given) * 1.0 / sum(unit_click), 4)
        |        else 0 end) as cpa_given,
        |  (case when sum(unit_cv) > 0 then round(sum(unit_cost) * 1.0 / sum(unit_cv), 4)
        |        else 0 end) as cpa_real,
        |  (case when sum(unit_uid_num) > 0 then round(sum(unit_cost) * 1000.0 / sum(unit_uid_num), 4)
        |        else 0 end) as arpu
        |from
        |  (select * from mingtou_index_table
        |  union
        |  select * from antou_index_table) temp
        |group by
        |  type
      """.stripMargin
    println("--------get all index value sql2--------")
    println(sql2)
    spark.sql(sql2).createOrReplaceTempView("all_index_table")

    // 合并整体和分行业的
    val sql3 =
      s"""
        |select * from all_index_table
        |union
        |select * from industry_index_table
      """.stripMargin
    println("--------get all index value sql3--------")
    println(sql3)
    val dataDF = spark.sql(sql3)
    dataDF
  }
}
