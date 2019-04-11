package com.cpc.spark.OcpcProtoType.ocpcCostEveryIndustry

import org.apache.spark.sql.{DataFrame, SparkSession}

object GetYesterdayOcpcCost {
  def main(args: Array[String]): Unit = {
    val today = args(0).toString
    val yesterday = GetPreDate.getPreDate(today)
    val spark = SparkSession.builder().appName("GetYesterdayOcpcCost").enableHiveSupport().getOrCreate()
    getYesterdayOcpcCost(today, yesterday, spark)
  }

  def getYesterdayOcpcCost(today: String, yesterday: String, spark: SparkSession): DataFrame ={
    val sql1 =
      s"""
         |select
         |  *
         |from
         |  dl_cpc.ocpc_cost_every_industry_base_data
         |where
         |  `date` = '$today'
      """.stripMargin
    println("------ GetYesterdayOcpcCost： get yesterday ocpc cost sql1 -------")
    println(sql1)
    val baseDataDF = spark.sql(sql1)

    getAllYesterdayOcpcCost(yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table1")
    getIndustryYesterdayOcpcCost(yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table2")
    getChiTuEtcYesterdayOcpcCost(yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table3")
    getApiCallBackYesterdayOcpcCost(yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table4")

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
    println("------ GetYesterdayOcpcCost： get yesterday ocpc cost sql2 -------")
    println(sql2)
    val yesterdayOcpcCostDF = spark.sql(sql2)
    yesterdayOcpcCostDF.write.mode("overwrite").saveAsTable("test.wt_ocpc_cost_every_industry_table4")
    yesterdayOcpcCostDF
  }

  // 昨天整体的ocpc消费
  def getAllYesterdayOcpcCost(yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame ={
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    'all' as industry,
        |    sum(case when is_ocpc = 1 then cost else 0 end) as ocpc_cost_yesterday,
        |from
        |    base_data_table
        |where
        |    dt = '$yesterday'
        |group by
        |    'all'
      """.stripMargin
    println("------ GetYesterdayOcpcCost： get all yesterday's ocpc cost -------")
    println(sql)
    val allYesterdayOcpcCostDF = spark.sql(sql)
    allYesterdayOcpcCostDF
  }

  // 昨天分行业的ocpc消费
  def getIndustryYesterdayOcpcCost(yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame = {
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    industry,
        |    sum(case when is_ocpc = 1 then cost else 0 end) as ocpc_cost_yesterday,
        |from
        |    base_data_table
        |where
        |    dt = '$yesterday'
        |group by
        |    industry
      """.stripMargin
    println("------ GetYesterdayOcpcCost： get industry yesterday's ocpc cost -------")
    println(sql)
    val industryYesterdayOcpcCostDF = spark.sql(sql)
    industryYesterdayOcpcCostDF
  }

  // 昨天赤兔、建站、非建站的ocpc消费
  def getChiTuEtcYesterdayOcpcCost(yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame = {
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    (case when siteid > 0 and siteid > 5000000 then 'elds_chitu'
        |          when siteid > 0 then 'elds_jianzhan'
        |         else 'elds_notjianzhan' end) as industry,
        |    sum(case when is_ocpc = 1 then cost else 0 end) as ocpc_cost_yesterday,
        |from
        |    base_data_table
        |where
        |    dt = '$yesterday'
        |and
        |    industry = 'elds'
        |group by
        |    (case when siteid > 0 and siteid > 5000000 then 'elds_chitu'
        |          when siteid > 0 then 'elds_jianzhan'
        |         else 'elds_notjianzhan' end)
      """.stripMargin
    println("------ GetYesterdayOcpcCost： get chitu jianzhan notjianzhan yesterday's ocpc cost -------")
    println(sql)
    val chiTuEtcYesterdayOcpcCostDF = spark.sql(sql)
    chiTuEtcYesterdayOcpcCostDF
  }

  // 昨天app的 api_callback 的ocpc消费
  def getApiCallBackYesterdayOcpcCost(yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame = {
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    'app_api_callback' as industry,
        |    sum(case when is_ocpc = 1 then cost else 0 end) as ocpc_cost_yesterday,
        |from
        |    base_data_table
        |where
        |    dt = '$yesterday'
        |and
        |    industry = 'app'
        |and
        |    is_api_callback = 1
        |group by
        |    'app_api_callback'
      """.stripMargin
    println("------ GetYesterdayOcpcCost： get api callback yesterday's ocpc cost -------")
    println(sql)
    val apiCallBackYesterdayOcpcCostDF = spark.sql(sql)
    apiCallBackYesterdayOcpcCostDF
  }
}
