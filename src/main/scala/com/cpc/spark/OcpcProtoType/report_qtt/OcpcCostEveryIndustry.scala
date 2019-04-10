package com.cpc.spark.OcpcProtoType.report_qtt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcCostEveryIndustry {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("OcpcCostEveryIndustry").enableHiveSupport().getOrCreate()
    getOcpcCost(date, spark)
    println("--------success----------")
  }

  def getOcpcCost(date: String, spark: SparkSession): Unit ={
    // 统计整体的数据
    val sql1 =
      s"""
        |select
        |    (case when (cast(adclass as string) like "134%" or cast(adclass as string) like "107%") then 'elds'
        |          when cast(adclass as string) like "100%" then 'app'
        |          when adclass in (110110100, 125100100) then 'wzcp'
        |          else 'others' end) as industry,
        |    sum(isclick) as all_click,
        |    sum(case when isclick = 1 and is_ocpc = 1 then 1 else 0 end) as ocpc_click,
        |    sum(isshow) as all_show,
        |    sum(case when isshow = 1 and is_ocpc = 1 then 1 else 0 end) as ocpc_show,
        |    sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 0.01 as ocpc_cost,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as all_cost,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end)
        |      / sum(case when isclick = 1 then price else 0 end), 3) as ratio,
        |    round(sum(case when isclick = 1 then price else 0 end) * 10.0
        |      / sum(isshow), 3) as all_cpm,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 10.0
        |      / sum(case when isshow = 1 and is_ocpc = 1 then 1 else 0 end), 3) as ocpc_cpm
        |from
        |    dl_cpc.ocpc_base_unionlog
        |where
        |    `date` = '$date'
        |and
        |    media_appsid  in ("80000001", "80000002")
        |and
        |    isshow = 1
        |and
        |    antispam = 0
        |and
        |    adsrc = 1
        |and
        |    (charge_type = 1 or charge_type is null)
        |group by
        |    `date`,
        |    (case when (cast(adclass as string) like "134%" or cast(adclass as string) like "107%") then 'elds'
        |          when cast(adclass as string) like "100%" then 'app'
        |          when adclass in (110110100, 125100100) then 'wzcp'
        |          else 'others' end)
      """.stripMargin
    println("-------all data sql1--------")
    println(sql1)
    spark.sql(sql1).createOrReplaceTempView("temp_table1")

    // 统建建站和非建站的数据
    val sql2 =
      s"""
        |select
        |    (case when siteid > 0 then 'elds_jianzhan'
        |          else 'elds_feijianzhan' end) as industry,
        |    sum(isclick) as all_click,
        |    sum(case when isclick = 1 and is_ocpc = 1 then 1 else 0 end) as ocpc_click,
        |    sum(isshow) as all_show,
        |    sum(case when isshow = 1 and is_ocpc = 1 then 1 else 0 end) as ocpc_show,
        |    sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 0.01 as ocpc_cost,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as all_cost,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end)
        |      / sum(case when isclick = 1 then price else 0 end), 3) as ratio,
        |    round(sum(case when isclick = 1 then price else 0 end) * 10.0
        |      / sum(isshow), 3) as all_cpm,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 10.0
        |      / sum(case when isshow = 1 and is_ocpc = 1 then 1 else 0 end), 3) as ocpc_cpm
        |from
        |    dl_cpc.ocpc_base_unionlog
        |where
        |    `date` = '$date'
        |and
        |    media_appsid  in ("80000001", "80000002")
        |and
        |    isshow = 1
        |and
        |    antispam = 0
        |and
        |    adsrc = 1
        |and
        |    (charge_type = 1 or charge_type is null)
        |and
        |    (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
        |group by
        |    (case when siteid > 0 then 'elds_jianzhan'
        |          else 'elds_feijianzhan' end)
      """.stripMargin
    println("-------jianzhan and feijianzhan sql2--------")
    println(sql2)
    spark.sql(sql2).createOrReplaceTempView("temp_table2")

    // 统计赤兔的数据
    val sql3 =
      s"""
        |select
        |    'elds_chitu' as industry,
        |    sum(isclick) as all_click,
        |    sum(case when isclick = 1 and is_ocpc = 1 then 1 else 0 end) as ocpc_click,
        |    sum(isshow) as all_show,
        |    sum(case when isshow = 1 and is_ocpc = 1 then 1 else 0 end) as ocpc_show,
        |    sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 0.01 as ocpc_cost,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as all_cost,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end)
        |      / sum(case when isclick = 1 then price else 0 end), 3) as ratio,
        |    round(sum(case when isclick = 1 then price else 0 end) * 10.0
        |      / sum(isshow), 3) as all_cpm,
        |    round(sum(case when isclick = 1 and is_ocpc = 1 then price else 0 end) * 10.0
        |      / sum(case when isshow = 1 and is_ocpc = 1 then 1 else 0 end), 3) as ocpc_cpm
        |from
        |    dl_cpc.ocpc_base_unionlog
        |where
        |    `date` = '$date'
        |and
        |    media_appsid  in ("80000001", "80000002")
        |and
        |    isshow = 1
        |and
        |    antispam = 0
        |and
        |    adsrc = 1
        |and
        |    (charge_type = 1 or charge_type is null)
        |and
        |    (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
        |and
        |    siteid > 5000000
        |group by
        |    'elds_chitu'
      """.stripMargin
    println("-------chitu sql3--------")
    println(sql3)
    spark.sql(sql3).createOrReplaceTempView("temp_table3")

    // 合并总的数据
    val sql4 =
      s"""
        |select * from
        |    (select * from temp_table1
        |    union
        |    select * from temp_table2
        |    union
        |    select * from temp_table3)
        |order by
        |    industry
      """.stripMargin
    println("-------merge all data--------")
    println(sql4)
    val dataDF = spark.sql(sql4)
    dataDF
      .withColumn("date", lit(date))
      .withColumn("version", lit("qtt_demo"))
      .repartition(2)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_cost_every_industry_data")
  }
}
