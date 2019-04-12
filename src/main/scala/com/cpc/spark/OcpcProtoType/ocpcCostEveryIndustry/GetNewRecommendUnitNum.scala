package com.cpc.spark.OcpcProtoType.ocpcCostEveryIndustry

import org.apache.spark.sql.{DataFrame, SparkSession}

object GetNewRecommendUnitNum {
  def main(args: Array[String]): Unit = {
    val today = args(0).toString
    val spark = SparkSession.builder().appName("GetNewRecommendUnitNum").enableHiveSupport().getOrCreate()
    getNewRecommendUnitNum(today, spark)
    println("------- has got new reommend unit num ------")
  }

  // 从suggest表里面获得当日新准入的单元数
  // 仅限于elds、app_api_callback、others、wzcp
  def getNewRecommendUnitNum(today: String, spark: SparkSession): DataFrame ={
    val sql1 =
      s"""
        |select
        |    unitid,
        |    industry,
        |    conversion_goal
        |from
        |    dl_cpc.ocpc_suggest_cpa_recommend_hourly
        |where
        |    `date` = '$today'
        |and
        |    hour = '06'
        |and
        |    version = 'qtt_demo'
        |and
        |    is_recommend = 1
        |and
        |    ocpc_flag = 0
        |and
        |    conversion_goal in (1, 2, 3)
        |group by
        |    unitid,
        |    industry,
        |    conversion_goal
      """.stripMargin
    println("---- GetNewRecommendUnitNum：sql1 ----")
    println(sql1)
    spark.sql(sql1).createOrReplaceTempView("base_data_table")

    val sql2 =
      s"""
        |select
        |    'all' as industry,
        |    count(unitid) as recommend_unit
        |from
        |    base_data_table
        |group by
        |    'all'
        |
        |union
        |
        |select
        |    industry,
        |    count(unitid) as recommend_unit
        |from
        |    base_data_table
        |where
        |    industry = 'elds'
        |and
        |    conversion_goal = 3
        |group by
        |    industry
        |
        |union
        |
        |select
        |    'app_api_callback' as industry,
        |    count(unitid) as recommend_unit
        |from
        |    base_data_table
        |where
        |    industry = 'feedapp'
        |and
        |    conversion_goal = 2
        |group by
        |    'app_api_callback'
        |
        |union
        |
        |select
        |    industry,
        |    count(unitid) as recommend_unit
        |from
        |    base_data_table
        |where
        |    industry in ('others', 'wzcp')
        |and
        |    conversion_goal = 1
        |group by
        |    industry
      """.stripMargin
    println("---- GetNewRecommendUnitNum：sql2 ----")
    println(sql2)
    val newRecommendUnitNumDF = spark.sql(sql2)
    newRecommendUnitNumDF.write.mode("overwrite").saveAsTable("test.wt_ocpc_cost_every_industry_table5")
    newRecommendUnitNumDF
  }
}
