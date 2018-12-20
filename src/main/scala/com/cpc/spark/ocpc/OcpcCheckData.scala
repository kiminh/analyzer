package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcCheckData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString

    val sqlRequest =
      s"""
         |select
         |    uid,
         |    timestamp,
         |    searchid,
         |    userid,
         |    ext['exp_ctr'].int_value * 1.0 / 1000000 as exp_ctr,
         |    ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |    isclick,
         |    isshow,
         |    ideaid,
         |    exptags,
         |    price,
         |    ext_int['bid_ocpc'] as bid_ocpc,
         |    ext_int['is_ocpc'] as is_ocpc,
         |    ext_string['ocpc_log'] as ocpc_log
         |from
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
         |and isshow=1
         |and adslot_type in (1,2,3)
         |and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
         |and ideaid in (2450708,2450542,2450447,2432700,2374508,2442830)
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    data.write.mode("overwrite").saveAsTable("test.ocpc_check_ideaid_ctr20181220_hourly_bak")
  }
}
