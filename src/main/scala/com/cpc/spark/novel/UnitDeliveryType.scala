package com.cpc.spark.novel

import org.apache.spark.sql.SparkSession

object UnitDeliveryType {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val hour = args(1)
    val spark = SparkSession.builder()
      .appName(s"NovelUnionLog date = $date and hour = $hour")
      .enableHiveSupport()
      .getOrCreate()
    val sql =
      s"""
         |select unitid,delivery_type,day,hour
         |from dl_cpc.cpc_novel_union_events
         |where day= '$date' and hour = '$hour'
             """.stripMargin

    println(sql)
  }
}
