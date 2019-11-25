package com.cpc.spark.oCPX.basedata

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object OcpcHiddenBudgetTemp {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val tableName = args(0).toString
    println(s"table name is $tableName")
    
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
         |    industry not in ('wzcp')
         |""".stripMargin
    val data = spark.sql(sqlRequest)
    data
      .repartition(1)
      .write.mode("overwrite").saveAsTable(s"test.$tableName")
  }


}



