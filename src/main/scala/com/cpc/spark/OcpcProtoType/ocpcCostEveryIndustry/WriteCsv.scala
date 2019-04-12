package com.cpc.spark.OcpcProtoType.ocpcCostEveryIndustry

import org.apache.spark.sql.{DataFrame, SparkSession}

object WriteCsv {
  def main(args: Array[String]): Unit = {

  }

  def writeCsv(today: String, dataDF: DataFrame, spark: SparkSession): Unit ={
    val title = spark.sparkContext.parallelize(Seq(Seq("industry", "ocpc_show", "all_show",
    "ocpc_click", "all_click", "ocpc_cost", "all_cost", "cost_ratio", "ocpc_cost_yesterday",
    "ocpc_cost_ring_ratio", "all_unit_yesterday", "all_unit_today", "ocpc_unit_yesterday",
    "ocpc_unit_today", "new_ocpc_unit", "recommend_unit", "date").mkString(","))).map(x => (x, 1))
    val sortDataDF = dataDF.na.fill(0)
    val data = title.union(sortDataDF.rdd.map(x => Seq(x.getAs[String]("industry").toString,
      x.getAs[Int]("ocpc_show").toString, x.getAs[Int]("all_show").toString,
      x.getAs[Int]("ocpc_click").toString, x.getAs[Int]("all_click").toString,
      x.getAs[Double]("ocpc_cost").toString, x.getAs[Double]("all_cost").toString,
      x.getAs[Double]("cost_ratio").toString, x.getAs[Double]("ocpc_cost_yesterday").toString,
      x.getAs[Double]("ocpc_cost_ring_ratio").toString, x.getAs[Int]("all_unit_yesterday").toString,
      x.getAs[Int]("all_unit_today").toString, x.getAs[Int]("ocpc_unit_yesterday").toString,
      x.getAs[Int]("ocpc_unit_today").toString, x.getAs[Int]("new_ocpc_unit").toString,
      x.getAs[Int]("recommend_unit").toString, x.getAs[String]("date").toString).mkString(","))
      .map(x => (x, 2)))
      .sortBy(x => (x._2, x._1))
      .map(x => x._1)
    val list = data.collect()
    for(item <- list) println(item)
//    data
//      .repartition(1)
//      .saveAsTextFile(s"/user/cpc/wentao/ocpc_cost_every_industry_report/$today")
  }
}
