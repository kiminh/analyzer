package com.cpc.spark.OcpcProtoType.ocpcCostEveryIndustry

import org.apache.spark.sql.{DataFrame, SparkSession}

object GetNewOcpcUnitNum {
  def main(args: Array[String]): Unit = {
    val today = args(0).toString
    val yesterday = GetPreDate.getPreDate(today)
    val spark = SparkSession.builder().appName("GetBaseIndex").enableHiveSupport().getOrCreate()
    getNewOcpcUnitNum(today, yesterday, spark)
  }

  def getNewOcpcUnitNum(today: String, yesterday: String, spark: SparkSession): DataFrame = {
    val sql1 =
      s"""
         |select
         |  *
         |from
         |  dl_cpc.ocpc_cost_every_industry_base_data
         |where
         |  `date` = '$today'
      """.stripMargin
    println("------ GetNewOcpcUnitNum： get base index sql1 -------")
    println(sql1)
    val baseDataDF = spark.sql(sql1)
    getAllNewOcpcUnitNum(today, yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table1")
    getIndustryNewOcpcUnitNum(today, yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table2")
    getChiTuEtcNewOcpcUnitNum(today, yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table3")
    getApiCallBackNewOcpcUnitNum(today, yesterday, baseDataDF, spark).createOrReplaceTempView("temp_table4")

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
    println("------ GetNewOcpcUnitNum： get unit num sql2 -------")
    println(sql2)
    val newOcpcUnitNumDF = spark.sql(sql2)
    newOcpcUnitNumDF.write.mode("overwrite").saveAsTable("test.wt_ocpc_cost_every_industry_table3")
    newOcpcUnitNumDF
  }

    // 统计今天新增的总体的ocpc单元数
  def getAllNewOcpcUnitNum(today: String, yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame ={
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    'all' as industry,
        |    sum(case when unitid is null then 1 else 0 end) as new_unit
        |from
        |    (select
        |        b.unitid,
        |        b.industry
        |    from
        |        (select
        |            unitid,
        |            industry
        |        from
        |            base_data_table
        |        where
        |            dt = '$today'
        |        and
        |            is_ocpc = 1
        |        group by
        |            unitid) a
        |    left join
        |        (select
        |            unitid,
        |            industry
        |        from
        |            base_data_table
        |        where
        |            dt = '$yesterday'
        |        and
        |            is_ocpc = 1
        |        group by
        |            unitid) b
        |    on
        |        a.unitid = b.unitid
        |    and
        |        a.industry = b.industry) temp
        |group by
        |    'all'
      """.stripMargin
    println("------ GetNewOcpcUnitNum： get all new ocpc unit num -------")
    println(sql)
    val allNewOcpcUnitNumDF = spark.sql(sql)
    allNewOcpcUnitNumDF
  }

  // 统计今天新增的分行业的ocpc单元数
  def getIndustryNewOcpcUnitNum(today: String, yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame = {
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    industry,
        |    sum(case when unitid is null then 1 else 0 end) as new_unit
        |from
        |    (select
        |        b.unitid,
        |        a.industry
        |    from
        |        (select
        |            unitid,
        |            industry
        |        from
        |            base_data_table
        |        where
        |            dt = '$today'
        |        and
        |            is_ocpc = 1
        |        group by
        |            unitid,
        |            industry) a
        |    left join
        |        (select
        |            unitid,
        |            industry
        |        from
        |            base_data_table
        |        where
        |            dt = '$yesterday'
        |        and
        |            is_ocpc = 1
        |        group by
        |            unitid,
        |            industry) b
        |    on
        |        a.unitid = b.unitid
        |    and
        |        a.industry = b.industry) temp
        |group by
        |    industry
      """.stripMargin
    println("------ GetNewOcpcUnitNum： get industry new ocpc unit num -------")
    println(sql)
    val industryNewOcpcUnitNumDF = spark.sql(sql)
    industryNewOcpcUnitNumDF
  }

  // 统计赤兔、建站、非建站的今天新增的ocpc单元数
  def getChiTuEtcNewOcpcUnitNum(today: String, yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame = {
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    industry,
        |    sum(case when unitid is null then 1 else 0 end) as new_unit
        |from
        |    (select
        |        b.unitid,
        |        a.industry
        |    from
        |        (select
        |            (case when siteid > 0 then 'elds_jianzhan'
        |                  else 'elds_notjianzhan' end) as industry,
        |            unitid
        |        from
        |            base_data_table
        |        where
        |            dt = '$today'
        |        and
        |            is_ocpc = 1
        |        and
        |            industry = 'elds'
        |        group by
        |            (case when siteid > 0 then 'elds_jianzhan'
        |                  else 'elds_notjianzhan' end),
        |            unitid) a
        |    left join
        |        (select
        |            (case when siteid > 0 then 'elds_jianzhan'
        |                  else 'elds_notjianzhan' end) as industry,
        |            unitid
        |        from
        |            base_data_table
        |        where
        |            dt = '$yesterday'
        |        and
        |            is_ocpc = 1
        |        and
        |            industry = 'elds'
        |        group by
        |            (case when siteid > 0 then 'elds_jianzhan'
        |                  else 'elds_notjianzhan' end),
        |            unitid) b
        |    on
        |        a.unitid = b.unitid
        |    and
        |        a.industry = b.industry) temp
        |group by
        |    industry
        |
 |union
        |
 |select
        |    industry,
        |    sum(case when unitid is null then 1 else 0 end) as new_unit
        |from
        |    (select
        |        b.unitid,
        |        a.industry
        |    from
        |        (select
        |            'elds_chitu' as industry,
        |            unitid
        |        from
        |            base_data_table
        |        where
        |            dt = '$today'
        |        and
        |            is_ocpc = 1
        |        and
        |            industry = 'elds'
        |        and
        |            siteid > 5000000
        |        group by
        |            'elds_chitu',
        |            unitid) a
        |    left join
        |        (select
        |            'elds_chitu' as industry,
        |            unitid
        |        from
        |            base_data_table
        |        where
        |            dt = '$yesterday'
        |        and
        |            is_ocpc = 1
        |        and
        |            industry = 'elds'
        |        and
        |            siteid > 5000000
        |        group by
        |            'elds_chitu',
        |            unitid) b
        |    on
        |        a.unitid = b.unitid
        |    and
        |        a.industry = b.industry) temp
        |group by
        |    industry
      """.stripMargin
    println("------ GetNewOcpcUnitNum： get chitu jianzhan notjianzhan new ocpc unit num -------")
    println(sql)
    val chiTuEtcNewOcpcUnitNumDF = spark.sql(sql)
    chiTuEtcNewOcpcUnitNumDF
  }

  def getApiCallBackNewOcpcUnitNum(today: String, yesterday: String, baseDataDF: DataFrame, spark: SparkSession): DataFrame = {
    baseDataDF.createOrReplaceTempView("base_data_table")
    val sql =
      s"""
        |select
        |    'app_api_callback' as industry,
        |    sum(case when unitid is null then 1 else 0 end) as new_unit
        |from
        |    (select
        |        b.unitid
        |    from
        |        (select
        |            unitid
        |        from
        |            base_data_table
        |        where
        |            dt = '$today'
        |        and
        |            is_ocpc = 1
        |        and
        |            industry = 'app'
        |        and
        |            is_api_callback = 1
        |        group by
        |            unitid) a
        |    left join
        |        (select
        |            unitid
        |        from
        |            base_data_table
        |        where
        |            dt = '$yesterday'
        |        and
        |            is_ocpc = 1
        |        and
        |            industry = 'app'
        |        and
        |            is_api_callback = 1
        |        group by
        |            unitid) b
        |    on
        |        a.unitid = b.unitid) temp
        |group by
        |    'app_api_callback'
      """.stripMargin
    println("------ GetNewOcpcUnitNum： get cpi callback new ocpc unit num -------")
    println(sql)
    val cpiCallBcakNewOcpcUnitNumDF = spark.sql(sql)
    cpiCallBcakNewOcpcUnitNumDF
  }
}
