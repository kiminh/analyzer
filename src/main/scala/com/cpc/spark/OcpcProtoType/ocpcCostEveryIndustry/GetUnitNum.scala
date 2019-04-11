package com.cpc.spark.OcpcProtoType.ocpcCostEveryIndustry

import org.apache.spark.sql.{DataFrame, SparkSession}

object GetUnitNum {
  def main(args: Array[String]): Unit = {
    val today = args(0).toString
    val yesterday = GetPreDate(today)
    val spark = SparkSession.builder().appName("GetBaseIndex").enableHiveSupport().getOrCreate()
    getUnitNum(today, yesterday, spark)
  }

  def getUnitNum(today: String, yesterday: String, spark: SparkSession): DataFrame ={
    val sql1 =
      s"""
         |select
         |  *
         |from
         |  dl_cpc.ocpc_cost_every_industry_base_data
         |where
         |  `date` = '$today'
      """.stripMargin
    println("------ GetUnitNum： get base index sql1 -------")
    println(sql1)
    val baseDataDF = spark.sql(sql1)

    getAllUnitNum(today, yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table1")
    getIndustryUnitNum(today, yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table2")
    getChiTuEtcUnitNum(today, yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table3")
    getApiCallBackUnitNum(today, yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table4")

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
    println("------ GetUnitNum： get unit num sql2 -------")
    println(sql2)
    val unitNumDF = spark.sql(sql2)
    unitNumDF.write.mode("overwrite").saveAsTable("test.wt_ocpc_cost_every_industry_table2")
    unitNumDF
  }

  // 统计总体的昨天和今天的总单元数
  def getAllUnitNum(today: String, yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame ={
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    a.industry,
        |    a.all_unit_yesterday,
        |    b.all_unit_today,
        |    c.ocpc_unit_yesterday,
        |    c.ocpc_unit_today
        |from
        |    (select
        |        'all' as industry,
        |        count(distinct unitid) as all_unit_yesterday
        |    from
        |        base_data_table
        |    where
        |        `date` = '$yesterday'
        |    group by
        |        'all') a
        |left join
        |    (select
        |        'all' as industry,
        |        count(distinct unitid) as all_unit_today
        |    from
        |        base_data_table
        |    where
        |        `date` = '$today'
        |    group by
        |        'all') b
        |on
        |    a.industry = b.industry
        |left join
        |    (select
        |        'all' as industry,
        |        count(distinct unitid) as ocpc_unit_yesterday
        |    from
        |        base_data_table
        |    where
        |        `date` = '$yesterday'
        |    and
        |        is_ocpc = 1
        |    group by
        |        'all') c
        |on
        |    a.industry = c.industry
        |left join
        |    (select
        |        'all' as industry,
        |        count(distinct unitid) as ocpc_unit_today
        |    from
        |        base_data_table
        |    where
        |        `date` = '$today'
        |    and
        |        is_ocpc = 1
        |    group by
        |        'all') d
        |on
        |    a.industry = d.industry
      """.stripMargin
    println("------ GetUnitNum： get all unit num -------")
    println(sql)
    val allUnitNumDF = spark.sql(sql)
    allUnitNumDF
  }

  // 统计分行业的昨天和今天的总单元数
  def getIndustryUnitNum(today: String, yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame = {
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    a.industry,
        |    a.all_unit_yesterday,
        |    b.all_unit_today,
        |    c.ocpc_unit_yesterday,
        |    d.ocpc_unit_today
        |from
        |    (select
        |        industry,
        |        count(distinct unitid) as all_unit_yesterday
        |    from
        |        base_data_table
        |    where
        |        `date` = '$yesterday'
        |    group by
        |        industry) a
        |left join
        |    (select
        |        industry,
        |        count(distinct unitid) as all_unit_today
        |    from
        |        base_data_table
        |    where
        |        `date` = '$today'
        |    group by
        |        industry) b
        |on
        |    a.industry = b.industry
        |left join
        |    (select
        |        industry,
        |        count(distinct unitid) as ocpc_unit_yesterday
        |    from
        |        base_data_table
        |    where
        |        `date` = '$yesterday'
        |    and
        |        is_ocpc = 1
        |    group by
        |        industry) c
        |on
        |    a.industry = c.industry
        |left join
        |    (select
        |        industry,
        |        count(distinct unitid) as ocpc_unit_today
        |    from
        |        base_data_table
        |    where
        |        `date` = '$today'
        |    and
        |        is_ocpc = 1
        |    group by
        |        industry) d
        |on
        |    a.industry = d.industry
      """.stripMargin
    println("------ GetUnitNum： get industry unit num -------")
    println(sql)
    val industryUnitNumDF = spark.sql(sql)
    industryUnitNumDF
  }

  // 统计赤兔、建站、非建站的总单元数
  def getChiTuEtcUnitNum(today: String, yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame = {
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    a.industry,
        |    a.all_unit_yesterday,
        |    b.all_unit_today,
        |    c.ocpc_unit_yesterday,
        |    d.ocpc_unit_today
        |from
        |    (select
        |        (case when siteid > 0 and siteid > 5000000 then 'elds_chitu'
        |              when siteid > 0 then 'elds_jianzhan'
        |              else 'elds_notjianzhan' end) as industry,
        |        count(distinct unitid) as all_unit_yesterday
        |    from
        |        base_data_table
        |    where
        |        `date` = '$yesterday'
        |    and
        |        industry = 'elds'
        |    group by
        |        (case when siteid > 0 and siteid > 5000000 then 'elds_chitu'
        |              when siteid > 0 then 'elds_jianzhan'
        |              else 'elds_notjianzhan' end)) a
        |left join
        |    (select
        |        (case when siteid > 0 and siteid > 5000000 then 'elds_chitu'
        |              when siteid > 0 then 'elds_jianzhan'
        |              else 'elds_notjianzhan' end) as industry,
        |        count(distinct unitid) as all_unit_today
        |    from
        |        base_data_table
        |    where
        |        `date` = '$today'
        |    and
        |        industry = 'elds'
        |    group by
        |        (case when siteid > 0 and siteid > 5000000 then 'elds_chitu'
        |              when siteid > 0 then 'elds_jianzhan'
        |              else 'elds_notjianzhan' end)) b
        |on
        |    a.industry = b.industry
        |left join
        |    (select
        |        (case when siteid > 0 and siteid > 5000000 then 'elds_chitu'
        |              when siteid > 0 then 'elds_jianzhan'
        |              else 'elds_notjianzhan' end) as industry,
        |        count(distinct unitid) as ocpc_unit_yesterday
        |    from
        |        base_data_table
        |    where
        |        `date` = '$yesterday'
        |    and
        |        industry = 'elds'
        |    and
        |        is_ocpc = 1
        |    group by
        |        (case when siteid > 0 and siteid > 5000000 then 'elds_chitu'
        |              when siteid > 0 then 'elds_jianzhan'
        |              else 'elds_notjianzhan' end)) c
        |on
        |    a.industry = c.industry
        |left join
        |    (select
        |        (case when siteid > 0 and siteid > 5000000 then 'elds_chitu'
        |              when siteid > 0 then 'elds_jianzhan'
        |              else 'elds_notjianzhan' end) as industry,
        |        count(distinct unitid) as ocpc_unit_today
        |    from
        |        base_data_table
        |    where
        |        `date` = '$today'
        |    and
        |        industry = 'elds'
        |    and
        |        is_ocpc = 1
        |    group by
        |        (case when siteid > 0 and siteid > 5000000 then 'elds_chitu'
        |              when siteid > 0 then 'elds_jianzhan'
        |              else 'elds_notjianzhan' end)) d
        |on
        |    a.industry = d.industry
      """.stripMargin
    println("------ GetUnitNum： get chitu jianzhan notjianzhan unit num -------")
    println(sql)
    val chiTuEtcUnitNumDF = spark.sql(sql)
    chiTuEtcUnitNumDF
  }

  // 统计app 的 api_callback的总单元数
  def getApiCallBackUnitNum(today: String, yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame = {
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    a.industry,
        |    a.all_unit_yesterday,
        |    b.all_unit_today,
        |    c.ocpc_unit_yesterday,
        |    c.ocpc_unit_today
        |from
        |    (select
        |        'app_api_callback' as industry,
        |        count(distinct unitid) as all_unit_yesterday
        |    from
        |        base_data_table
        |    where
        |        `date` = '$yesterday'
        |    and
        |        industry = 'app'
        |    and
        |        is_api_callback = 1
        |    group by
        |        'app_api_callback') a
        |left join
        |    (select
        |        'app_api_callback' as industry,
        |        count(distinct unitid) as all_unit_today
        |    from
        |        base_data_table
        |    where
        |        `date` = '$today'
        |    and
        |        industry = 'app'
        |    and
        |        is_api_callback = 1
        |    group by
        |        'app_api_callback') b
        |on
        |    a.industry = b.industry
        |left join
        |    (select
        |        'app_api_callback' as industry,
        |        count(distinct unitid) as ocpc_unit_yesterday
        |    from
        |        base_data_table
        |    where
        |        `date` = '$yesterday'
        |    and
        |        is_ocpc = 1
        |    and
        |        industry = 'app'
        |    and
        |        is_api_callback = 1
        |    group by
        |        'app_api_callback') c
        |on
        |    a.industry = c.industry
        |left join
        |    (select
        |        'app_api_callback' as industry,
        |        count(distinct unitid) as ocpc_unit_today
        |    from
        |        base_data_table
        |    where
        |        `date` = '$today'
        |    and
        |        is_ocpc = 1
        |    and
        |        industry = 'app'
        |    and
        |        is_api_callback = 1
        |    group by
        |        'app_api_callback') d
        |on
        |    a.industry = d.industry
      """.stripMargin
    println("------ GetUnitNum： get api callback unit num -------")
    println(sql)
    val apiCallBackUnitNumDF = spark.sql(sql)
    apiCallBackUnitNumDF
  }
}
