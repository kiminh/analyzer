package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcTraceAdType {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    traceAdType(date, hour, spark)

  }

  def traceAdType(date: String, hour: String, spark: SparkSession): Unit = {
    import spark.implicits._

    val sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    userid,
         |    ideaid,
         |    timestamp,
         |    ext['adclass'].int_value as adclass,
         |    ext_int['siteid'] as siteid
         |FROM
         |    dl_cpc.cpc_union_log
         |WHERE
         |    `date` = '$date'
         |and
         |    `hour` = '$hour'
         |and
         |    media_appsid  in ("80000001", "80000002")
         |and
         |    ext['antispam'].int_value = 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
       """.stripMargin

    println(sqlRequest)
    val unionlog = spark.sql(sqlRequest)
    println(unionlog.count)

    val ocpcAdList = spark.table("test.ocpc_idea_update_time_" + hour)
    println(ocpcAdList.count)

    val rawData = unionlog.join(ocpcAdList, Seq("ideaid")).select("ideaid", "adclass", "timestamp", "siteid")
    println(rawData.count)
    rawData.show(10)

//    rawData
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .write
//      .mode("overwrite")
//      .saveAsTable("test.ocpc_track_ad_type_hourly")


    rawData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_track_ad_type_hourly")


  }
}