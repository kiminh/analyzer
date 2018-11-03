package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcActivationData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcMonitor").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    getDataHourly(date, hour, spark)

  }
  def getDataHourly(date: String, hour: String, spark: SparkSession) :Unit = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"
    var sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  ext['adclass'].int_value as adclass,
         |  isclick,
         |  isshow,
         |  iscvr as isact
         |FROM
         |  dl_cpc.cpc_api_union_log
         |WHERE
         |  $selectWhere
      """.stripMargin
    println(sqlRequest)
    val base = spark.sql(sqlRequest)

    val resultDF = base
      .groupBy("ideaid", "adclass")
      .agg(
        sum(col("isshow")).alias("show_cnt"),
        sum(col("isclick")).alias("ctr_cnt"),
        sum(col("isact")).alias("cvr_cnt"))
      .select("ideaid", "adclass", "show_cnt", "ctr_cnt", "cvr_cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_ideaid_adclass_label3_track")

//    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_ideaid_adclass_label3_track")

  }
}