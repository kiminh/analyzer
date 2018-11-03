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
         |  a.trace_click_count as ideaid,
         |  1 as label
         |FROM
         |  dl_cpc.cpc_union_trace_logV2
      """.stripMargin
    println(sqlRequest)
    val base = spark.sql(sqlRequest)

    val resultDF = base
      .groupBy("ideaid")
      .agg(sum(col("label")).alias("cvr_cnt"))
      .select("ideaid", "cvr_cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_ideaid_adclass_label3_track")

//    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_ideaid_adclass_label3_track")

  }
}