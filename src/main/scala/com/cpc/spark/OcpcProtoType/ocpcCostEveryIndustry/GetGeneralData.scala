package com.cpc.spark.OcpcProtoType.ocpcCostEveryIndustry

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.lit

object GetGeneralData {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("GetAllIndex").enableHiveSupport().getOrCreate()
    // 先获取基础数据
    saveDataToCsv(date, spark)
    println("------- complete writing data --------")
  }

  def saveDataToCsv(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  industry,
         |  cost as ocpc_cost,
         |  cast(round(cost_cmp, 2) as double) as cost_cmp,
         |  cast(round(cost_ratio, 2) as double) as cost_ratio,
         |  cast(round(low_unit_percent, 2) as double) as low_unit_percent,
         |  cast(round(pay_percent, 2) as double) as pay_percent
         |FROM
         |  dl_cpc.ocpc_general_data_industry
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '23'
         |AND
         |  version = 'qtt_demo'
         |AND
         |  industry in ('elds', 'feedapp')
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data.show(10)

//    writeCsv(date, data, spark)

  }

  def writeCsv(date: String, dataDF: DataFrame, spark: SparkSession): Unit ={
    val title = spark.sparkContext.parallelize(Seq(Seq("industry", "ocpc_cost", "cost_cmp", "cost_ratio", "low_unit_percent", "pay_percent").mkString(","))).map(x => (x, 1))
    val sortDataDF = dataDF.na.fill(0)
    val data = title.union(sortDataDF.rdd.map(x => Seq(
      x.getAs[String]("industry").toString,
      x.getAs[Double]("ocpc_cost").toString,
      x.getAs[Double]("cost_cmp").toString,
      x.getAs[Double]("cost_ratio").toString,
      x.getAs[Double]("low_unit_percent").toString,
      x.getAs[Double]("pay_percent").toString).mkString(","))
      .map(x => (x, 2)))
      .repartition(1)
      .sortBy(x => (x._2, x._1))
      .map(x => x._1)
    val list = data.collect()
    for(item <- list) println(item + " " + item.split(",").length)
    data.saveAsTextFile(s"/user/cpc/wangjun/ocpc_cost_general/$date")
  }

//  def getAllIndex(today: String, yesterday: String, days7ago: String, spark: SparkSession): Unit ={
//    GetBaseIndex.getBaseIndex(today, spark).createOrReplaceTempView("base_index_table")
//    GetUnitNum.getUnitNum(today, yesterday, spark).createOrReplaceTempView("unit_num_table")
//    GetNewOcpcUnitNum.getNewOcpcUnitNum(today, yesterday, spark).createOrReplaceTempView("new_ocpc_unit_num_table")
//    GetPreOcpcCost.getYesterdayOcpcCost(today, yesterday, days7ago, spark).createOrReplaceTempView("yesterday_ocpc_cost_table")
//    GetNewRecommendUnitNum.getNewRecommendUnitNum(today, spark).createOrReplaceTempView("new_recommend_unit_num_table")
//
//    val sql =
//      s"""
//         |select
//         |    a.industry,
//         |    a.ocpc_show,
//         |    a.all_show,
//         |    a.ocpc_click,
//         |    a.all_click,
//         |    round(a.ocpc_cost, 2) as ocpc_cost,
//         |    round(a.all_cost, 2) as all_cost,
//         |    round(a.cost_ratio, 3) as cost_ratio,
//         |    round(d.all_cost_yesterday, 2) as all_cost_yesterday,
//         |    round(a.all_cost / d.all_cost_yesterday, 3) as all_cost_yesterday_ratio,
//         |    round(d.all_cost_days7ago, 2) as all_cost_days7ago,
//         |    round(a.all_cost / d.all_cost_days7ago, 3) as all_cost_days7ago_ratio,
//         |    round(d.ocpc_cost_yesterday, 2) as ocpc_cost_yesterday,
//         |    round(a.ocpc_cost / d.ocpc_cost_yesterday, 3) as ocpc_cost_yesterday_ratio,
//         |    round(d.ocpc_cost_days7ago, 2) as ocpc_cost_days7ago,
//         |    round(a.ocpc_cost / d.ocpc_cost_days7ago, 3) as ocpc_cost_days7ago_ratio,
//         |    b.all_unit_yesterday,
//         |    b.all_unit_today,
//         |    b.ocpc_unit_yesterday,
//         |    b.ocpc_unit_today,
//         |    c.new_unit as new_ocpc_unit,
//         |    e.recommend_unit
//         |from
//         |    base_index_table a
//         |left join
//         |    unit_num_table b
//         |on
//         |    a.industry = b.industry
//         |left join
//         |    new_ocpc_unit_num_table c
//         |on
//         |    a.industry = c.industry
//         |left join
//         |    yesterday_ocpc_cost_table d
//         |on
//         |    a.industry = d.industry
//         |left join
//         |    new_recommend_unit_num_table e
//         |on
//         |    a.industry = e.industry
//      """.stripMargin
//    println("--------- GetAllIndex：get all index -----------")
//    println(sql)
//    val dataDF = spark.sql(sql).withColumn("date", lit(today))
//    // 存hive表
//    dataDF
//      .repartition(10)
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_cost_every_industry")
//    println("------ save into hive success -----")
//    // 存hadoop文件
//    WriteCsv.writeCsv(today, dataDF, spark)
//    println("------ save into hadoop success -----")
//  }
}
