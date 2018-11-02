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
         | select
         |  a.searchid,
         |  a.uid,
         |  a.ideaid,
         |  a.price,
         |  a.userid,
         |  a.ext['adclass'].int_value as adclass,
         |  a.isclick,
         |  a.isshow,
         |  (case when b.label is not null then 1 else 0 end) as isact
         | from
         |      (
         |        select *
         |        from dl_cpc.cpc_union_log
         |        where $selectWhere
         |        and isclick is not null
         |        and media_appsid  in ("80000001", "80000002")
         |        and isshow = 1
         |        and ext['antispam'].int_value = 0
         |        and ideaid > 0
         |        and adsrc = 1
         |        and adslot_type in (1,2,3)
         |      ) a
         |left join
         |      (
         |        select searchid, searchid as label
         |        from dl_cpc.cpc_union_trace_logV2
         |        where $selectWhere
         |      ) b on a.searchid = b.searchid
      """.stripMargin
    println(sqlRequest)
    val base = spark.sql(sqlRequest)

    val resultDF = base
      .groupBy("ideaid", "adclass")
      .agg(
        sum(col("isshow")).alias("show_cnt"),
        sum(col("isclick")).alias("ctr_cnt"),
        sum(col("isact")).alias("act_cnt"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_ideaid_adclass_label3_track")

    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpc_ideaid_adclass_label3_track")

  }
}