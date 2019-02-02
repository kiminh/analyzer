package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcCheckData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString

    val result = program(date, hour, spark)
    result
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_unionlog")
  }

  def program(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_unionlog
         |WHERE
         |  `dt`='$date'
         |AND
         |  `hour`='$hour'
         |AND
         |  ideaid!=2531409
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
  }


}
