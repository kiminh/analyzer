package com.cpc.spark.OcpcProtoType.report_qtt

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcRatioInMarket {

  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("OcpcRatioInMarket").enableHiveSupport().getOrCreate()
    getOcpcRatio(date, spark)
    println("has got data")
  }

  def getOcpcRatio(date: String, spark: SparkSession): Unit ={
    val sql =
      s"""
        |select
        |    round(sum(case when a.is_ocpc = 1 then 1 else 0 end) * 1.0 / count(a.industry), 3) as all_ratio,
        |    round(sum(case when a.is_ocpc = 1 and a.industry = 'elds' then 1 else 0 end) * 1.0
        |    / sum(case when a.industry = 'elds' then 1 else 0 end), 3) as elds_ratio,
        |    round(sum(case when a.is_ocpc = 1 and a.industry = 'feedapp' then 1 else 0 end) * 1.0
        |    / sum(case when a.industry = 'feedapp' then 1 else 0 end), 3) as feedapp_ratio,
        |    round(sum(case when a.is_ocpc = 1 and a.industry = 'wz' then 1 else 0 end) * 1.0
        |    / sum(case when a.industry = 'wz' then 1 else 0 end), 3) as wz_ratio
        |from
        |    (select
        |        `date`,
        |        (case
        |            when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
        |            when (adslot_type <> 7 and cast(adclass as string) like '100%') then "feedapp"
        |            when (adslot_type = 7 and cast(adclass as string) like '100%') then "yysc"
        |            when adclass = 110110100 then "wz"
        |            else "others" end) as industry,
        |        is_ocpc
        |    from
        |        dl_cpc.ocpc_base_unionlog
        |    where
        |        `date` >= '$date'
        |    and
        |        media_appsid  in ("80000001", "80000002")
        |    and
        |        isshow = 1
        |    and
        |        isclick = 1
        |    and
        |        antispam = 0
        |    and
        |        adslot_type in (1, 2, 3)
        |    and
        |        adsrc = 1
        |    and
        |        (charge_type is null or charge_type = 1)) as a
      """.stripMargin
    val data = spark.sql(sql)
    data
      .withColumn("date", lit(date))
      .withColumn("version",lit("qtt_demo"))
      .repartition(1)
      .write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_market_ratio")
  }

}
