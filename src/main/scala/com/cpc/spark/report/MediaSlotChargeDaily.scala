package com.cpc.spark.report

import org.apache.spark.sql.SparkSession

object MediaSlotChargeDaily {
  def main(args: Array[String]): Unit = {
    val day=args(0)

    val spark=SparkSession.builder()
      .appName(" media slot charge hourly")
      .enableHiveSupport()
      .getOrCreate()

    val sql=
      s"""
         |select *
         |from dl_cpc.cpc_basedata_union_events
         |where day='$day' and adslotid >0
       """.stripMargin
  }
}

