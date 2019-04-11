package com.cpc.spark.OcpcProtoType.ocpcCostEveryIndustry

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

object GetAllIndex {
  def main(args: Array[String]): Unit = {
    val today = args(0).toString
    val yesterday = GetPreDate.getPreDate(today)
    val spark = SparkSession.builder().appName("GetAllIndex").enableHiveSupport().getOrCreate()
    getAllIndex(today, yesterday, spark)
    println("------- has got all index ----------")
  }
  def getAllIndex(today: String, yesterday: String, spark: SparkSession): Unit ={
    GetBaseIndex.getBaseIndex(today, spark).createOrReplaceTempView("base_index_table")
    GetUnitNum.getUnitNum(today, yesterday, spark).createOrReplaceTempView("unit_num_table")
    GetNewOcpcUnitNum.getNewOcpcUnitNum(today, yesterday, spark).createOrReplaceTempView("new_ocpc_unit_num_table")
    GetYesterdayOcpcCost.getYesterdayOcpcCost(today, yesterday, spark).createOrReplaceTempView("yesterday_ocpc_cost_table")

    val sql =
      s"""
        |select
        |    a.industry,
        |    a.ocpc_show,
        |    a.all_show,
        |    a.ocpc_click,
        |    a.all_click,
        |    a.ocpc_cost,
        |    a.all_cost,
        |    a.cost_ratio,
        |    d.ocpc_cost_yesterday,
        |    round(a.ocpc_cost / d.ocpc_cost_yesterday, 2) as ocpc_cost_ring_ratio,
        |    b.all_unit_yesterday,
        |    b.all_unit_today,
        |    b.ocpc_unit_yesterday,
        |    b.ocpc_unit_today,
        |    c.new_unit as new_ocpc_unit
        |from
        |    base_index_table a
        |left join
        |    unit_num_table b
        |on
        |    a.industry = b.industry
        |left join
        |    new_ocpc_unit_num_table c
        |on
        |    a.industry = c.industry
        |left join
        |    yesterday_ocpc_cost_table d
        |on
        |    a.industry = d.industry
      """.stripMargin
    println("--------- GetAllIndex：get all index -----------")
    println(sql)
    val dataDF = spark.sql(sql)
    dataDF
      .withColumn("date", lit(today))
      .repartition(10)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_cost_every_industry")
  }
}
