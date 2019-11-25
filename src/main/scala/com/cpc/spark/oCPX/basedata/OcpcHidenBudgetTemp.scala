package com.cpc.spark.oCPX.basedata

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcHidenBudgetTemp {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    
    val sqlRequest =
      s"""
         |select
         |    *
         |from
         |    dl_cpc.ocpc_auto_budget_hourly
         |WHERE
         |    date = '2019-07-04'
         |and
         |    hour = '06'
         |and
         |    version = 'qtt_demo'
         |and
         |    industry not in ('wzcp');
         |""".stripMargin
    val data = spark.sql(sqlRequest)
    data
      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.ocpc_auto_budget_hourly_test")
  }


}



