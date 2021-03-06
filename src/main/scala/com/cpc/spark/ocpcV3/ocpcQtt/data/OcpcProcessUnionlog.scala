
package com.cpc.spark.ocpcV3.ocpcQtt.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcProcessUnionlog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val resultDF = preprocessUnionlog(date, hour, spark)
    resultDF
      .repartition(10).write.mode("overwrite").insertInto("test.ocpcv3_qtt_ctr_data_hourly")
//    resultDF.write.mode("overwrite").insertInto("dl_cpc.ocpcv3_qtt_ctr_data_hourly")
    println("successfully save data into table dl_cpc.ocpcv3_ctr_data_hourly")
  }

  def preprocessUnionlog(date: String, hour: String, spark: SparkSession) = {
    var selectWhere = s"`date`='$date' and hour = '$hour'"

    var sqlRequest =
      s"""
         |select
         |    searchid,
         |    uid,
         |    ideaid,
         |    unitid,
         |    price,
         |    bid,
         |    userid,
         |    ext['adclass'].int_value as adclass,
         |    ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |    isclick,
         |    isshow,
         |    ext_int['is_api_callback'] as is_api_callback
         |from dl_cpc.cpc_union_log
         |where $selectWhere
         |and isclick is not null
         |and media_appsid in ("80000001", "80000002")
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
      """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)
    rawData.createOrReplaceTempView("raw_table")

    // 展现数、点击数、花费
    val sqlRequest1 =
      s"""
         |SELECT
         |  ideaid,
         |  unitid,
         |  adclass,
         |  SUM(case when isclick=1 then price else 0 end) as total_price,
         |  SUM(isshow) as show_cnt,
         |  SUM(isclick) as ctr_cnt,
         |  SUM(case when isclick=1 then bid else 0 end) as total_bid,
         |  SUM(case when isclick=1 then exp_cvr else 0 end) as total_pcvr
         |FROM
         |  raw_table
         |GROUP BY ideaid, unitid, adclass
       """.stripMargin
    println(sqlRequest1)
    val data = spark.sql(sqlRequest1)

    val resultDF = data
      .select("ideaid", "unitid", "adclass", "total_price", "show_cnt", "ctr_cnt", "total_bid", "total_pcvr")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF

  }

}


