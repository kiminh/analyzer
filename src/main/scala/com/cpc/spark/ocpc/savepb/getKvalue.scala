package com.cpc.spark.ocpc.savepb

import org.apache.spark.sql.SparkSession

object getKvalue {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val resultDF = spark.table("test.ocpc_k_value_table")
    // TODO: 换成dl_cpc的表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_kvalue_base_hourly")

  }

}