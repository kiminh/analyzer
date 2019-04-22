package com.cpc.spark.OcpcProtoType.ocpcCostEveryIndustry

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object GetBaseData {
  def main(args: Array[String]): Unit = {
    val today = args(0).toString
    val yesterday = GetPreDate.getPreDate(today)
    val days7ago = GetPreDate.getPreDate(today, 7)
    val spark = SparkSession.builder().appName("GetBaseData").enableHiveSupport().getOrCreate()
    getBaseData(today, yesterday, days7ago, spark)
    println("------ has got base data -------")
  }
  def getBaseData(today: String, yesterday: String, days7ago: String, spark: SparkSession): Unit ={
    val sql =
      s"""
        |select
        |    `date` as dt,
        |    unitid,
        |    is_ocpc,
        |    (case when (cast(adclass as string) like "134%" or cast(adclass as string) like "107%") then 'elds'
        |          when cast(adclass as string) like "100%" then 'app'
        |          when adclass in (110110100, 125100100) then 'wzcp'
        |          else 'others' end) as industry,
        |    siteid,
        |    is_api_callback,
        |    sum(isclick) as click,
        |    sum(isshow) as show,
        |    sum(case when isclick = 1 then price else 0 end) * 0.01 as cost
        |from
        |    dl_cpc.ocpc_base_unionlog
        |where
        |    `date` in ('$days7ago', '$yesterday', '$today')
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
        |    unitid,
        |    is_ocpc,
        |    (case when (cast(adclass as string) like "134%" or cast(adclass as string) like "107%") then 'elds'
        |          when cast(adclass as string) like "100%" then 'app'
        |          when adclass in (110110100, 125100100) then 'wzcp'
        |          else 'others' end),
        |    siteid,
        |    is_api_callback
      """.stripMargin
    println("----- get base data sql -------")
    println(sql)
    val data = spark.sql(sql)
//    data
//      .withColumn("date", lit(today))
//      .write.mode("overwrite").saveAsTable("test.wt_ocpc_cost_every_industry_base_data")
    data
      .withColumn("date", lit(today))
      .repartition(2)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_cost_every_industry_base_data")
  }
}
