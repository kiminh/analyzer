package com.cpc.spark.OcpcProtoType.ocpcCostEveryIndustry_hottopic

import org.apache.spark.sql.{DataFrame, SparkSession}

object GetBaseIndex {
  def main(args: Array[String]): Unit = {
    val today = args(0).toString
    val spark = SparkSession.builder().appName("GetBaseIndex").enableHiveSupport().getOrCreate()
    getBaseIndex(today, spark)
  }
  def getBaseIndex(today: String, spark: SparkSession): DataFrame ={
    val sql1 =
      s"""
        |select
        |  *
        |from
        |  dl_cpc.ocpc_cost_every_industry_base_data_hottopic
        |where
        |  `date` = '$today'
      """.stripMargin
    println("------ GetBaseIndex： get base index sql1 -------")
    println(sql1)
    val baseDataDF = spark.sql(sql1)
    // 创建临时表
    getAllBaseIndex(today, baseDataDF, spark).createOrReplaceTempView("temp_table1")
    getIndustryBaseIndex(today, baseDataDF, spark).createOrReplaceTempView("temp_table2")
    getChiTuEtcBaseIndex(today, baseDataDF, spark).createOrReplaceTempView("temp_table3")
    getApiCallBackBaseIndex(today, baseDataDF, spark).createOrReplaceTempView("temp_table4")

    val sql2 =
      s"""
        |select * from temp_table1
        |union
        |select * from temp_table2
        |union
        |select * from temp_table3
        |union
        |select * from temp_table4
      """.stripMargin
    println("------ GetBaseIndex： get base index sql2 -------")
    println(sql2)
    val baseIndexDF = spark.sql(sql2)
//    baseIndexDF.write.mode("overwrite").saveAsTable("test.wt_ocpc_cost_every_industry_table1")
    baseIndexDF
  }
  // 统计总体的指标：show、click、cost等
  def getAllBaseIndex(today: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame ={
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    'all' as industry,
        |    sum(show) as all_show,
        |    sum(case when is_ocpc = 1 then show else 0 end) as ocpc_show,
        |    sum(click) as all_click,
        |    sum(case when is_ocpc = 1 then click else 0 end) as ocpc_click,
        |    sum(cost) as all_cost,
        |    sum(case when is_ocpc = 1 then cost else 0 end) as ocpc_cost,
        |    round(sum(case when is_ocpc = 1 then cost else 0 end) / sum(cost), 4) as cost_ratio,
        |    round(sum(cost) * 1000.0 / sum(show), 2) as all_cpm,
        |    round(sum(case when is_ocpc = 1 then cost else 0 end) * 1000.0
        |        / sum(case when is_ocpc = 1 then show else 0 end), 2) as ocpc_cpm
        |from
        |    base_data_table
        |where
        |    dt = '$today'
        |group by
        |    'all'
      """.stripMargin
    println("------ GetBaseIndex： get all base index -------")
    println(sql)
    val allBaseIndexDF = spark.sql(sql)
    allBaseIndexDF
  }

  // 统计分行业的指标：show、click、cost等
  def getIndustryBaseIndex(today: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame ={
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    industry,
        |    sum(show) as all_show,
        |    sum(case when is_ocpc = 1 then show else 0 end) as ocpc_show,
        |    sum(click) as all_click,
        |    sum(case when is_ocpc = 1 then click else 0 end) as ocpc_click,
        |    sum(cost) as all_cost,
        |    sum(case when is_ocpc = 1 then cost else 0 end) as ocpc_cost,
        |    round(sum(case when is_ocpc = 1 then cost else 0 end) / sum(cost), 4) as cost_ratio,
        |    round(sum(cost) * 1000.0 / sum(show), 2) as all_cpm,
        |    round(sum(case when is_ocpc = 1 then cost else 0 end) * 1000.0
        |        / sum(case when is_ocpc = 1 then show else 0 end), 2) as ocpc_cpm
        |from
        |    base_data_table
        |where
        |    dt = '$today'
        |group by
        |    industry
      """.stripMargin
    println("------ GetBaseIndex： get industry base index -------")
    println(sql)
    val industryBaseIndexDF = spark.sql(sql)
    industryBaseIndexDF
  }

  // 统计赤兔、建站、非建站的指标：show、click、cost等
  def getChiTuEtcBaseIndex(today: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame ={
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    (case when siteid > 0 then 'elds_jianzhan'
        |          else 'elds_notjianzhan' end) as industry,
        |    sum(show) as all_show,
        |    sum(case when is_ocpc = 1 then show else 0 end) as ocpc_show,
        |    sum(click) as all_click,
        |    sum(case when is_ocpc = 1 then click else 0 end) as ocpc_click,
        |    sum(cost) as all_cost,
        |    sum(case when is_ocpc = 1 then cost else 0 end) as ocpc_cost,
        |    round(sum(case when is_ocpc = 1 then cost else 0 end) / sum(cost), 4) as cost_ratio,
        |    round(sum(cost) * 1000.0 / sum(show), 2) as all_cpm,
        |    round(sum(case when is_ocpc = 1 then cost else 0 end) * 1000.0
        |        / sum(case when is_ocpc = 1 then show else 0 end), 2) as ocpc_cpm
        |from
        |    base_data_table
        |where
        |    dt = '$today'
        |and
        |    industry = 'elds'
        |group by
        |    (case when siteid > 0 then 'elds_jianzhan'
        |          else 'elds_notjianzhan' end)
        |
        |union
        |
        |select
        |    'elds_chitu' as industry,
        |    sum(show) as all_show,
        |    sum(case when is_ocpc = 1 then show else 0 end) as ocpc_show,
        |    sum(click) as all_click,
        |    sum(case when is_ocpc = 1 then click else 0 end) as ocpc_click,
        |    sum(cost) as all_cost,
        |    sum(case when is_ocpc = 1 then cost else 0 end) as ocpc_cost,
        |    round(sum(case when is_ocpc = 1 then cost else 0 end) / sum(cost), 4) as cost_ratio,
        |    round(sum(cost) * 1000.0 / sum(show), 2) as all_cpm,
        |    round(sum(case when is_ocpc = 1 then cost else 0 end) * 1000.0
        |        / sum(case when is_ocpc = 1 then show else 0 end), 2) as ocpc_cpm
        |from
        |    base_data_table
        |where
        |    dt = '$today'
        |and
        |    industry = 'elds'
        |and
        |    siteid > 5000000
        |group by
        |    'elds_chitu'
      """.stripMargin
    println("------ GetBaseIndex： get chitu jianzhan notjianzhan base index -------")
    println(sql)
    val chituEtcBaseIndexDF = spark.sql(sql)
    chituEtcBaseIndexDF
  }

  // 统计app的api_callback的指标：show、click、cost等
  def getApiCallBackBaseIndex(today: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame ={
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    'app_api_callback' as industry,
        |    sum(show) as all_show,
        |    sum(case when is_ocpc = 1 then show else 0 end) as ocpc_show,
        |    sum(click) as all_click,
        |    sum(case when is_ocpc = 1 then click else 0 end) as ocpc_click,
        |    sum(cost) as all_cost,
        |    sum(case when is_ocpc = 1 then cost else 0 end) as ocpc_cost,
        |    round(sum(case when is_ocpc = 1 then cost else 0 end) / sum(cost), 4) as cost_ratio,
        |    round(sum(cost) * 1000.0 / sum(show), 2) as all_cpm,
        |    round(sum(case when is_ocpc = 1 then cost else 0 end) * 1000.0
        |        / sum(case when is_ocpc = 1 then show else 0 end), 2) as ocpc_cpm
        |from
        |    base_data_table
        |where
        |    dt = '$today'
        |and
        |    industry = 'app'
        |and
        |    is_api_callback = 1
        |group by
        |    'app_api_callback'
      """.stripMargin
    println("------ GetBaseIndex： get app api callback base index -------")
    println(sql)
    val apiCallbackBaseIndexDF = spark.sql(sql)
    apiCallbackBaseIndexDF
  }
}
