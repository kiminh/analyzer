
package com.cpc.spark.ocpcV3.ocpc.data

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.ocpc.utils.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap

object OcpcBaseCtr {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val resultDF = preprocessUnionlog(date, hour, spark)
    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_ctr_data_hourly")
//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_ctr_data_hourly")
    println("successfully save data into table dl_cpc.ocpc_ctr_data_hourly")
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
         |    adslotid,
         |    price,
         |    bid as original_bid,
         |    userid,
         |    media_type,
         |    media_appsid,
         |    adslot_type,
         |    adtype,
         |    ext['adclass'].int_value as adclass,
         |    ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |    isclick,
         |    isshow,
         |    ext_string['ocpc_log'] as ocpc_log
         |from dl_cpc.cpc_union_log
         |where $selectWhere
         |and isclick is not null
         |and media_appsid in ("80001098","80001292","80000001", "80000002", "80002819")
         |and isshow = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
      """.stripMargin
    println(sqlRequest)
    val base = spark
      .sql(sqlRequest)
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
    base.createOrReplaceTempView("base_table")
    val sqlRequestBase =
      s"""
         |select
         |    searchid,
         |    uid,
         |    ideaid,
         |    unitid,
         |    adslotid,
         |    price,
         |    original_bid,
         |    userid,
         |    media_type,
         |    media_appsid,
         |    adslot_type,
         |    adtype,
         |    adclass,
         |    cast(exp_cvr as double) as exp_cvr,
         |    isclick,
         |    isshow,
         |    ocpc_log,
         |    ocpc_log_dict,
         |    (case when length(ocpc_log)>0 then cast(ocpc_log_dict['dynamicbid'] as int) else original_bid end) as bid
         |from base_table
       """.stripMargin
    println(sqlRequestBase)
    val rawData = spark.sql(sqlRequestBase)
    rawData.createOrReplaceTempView("raw_table")

    // 展现数、点击数、花费
    val sqlRequest1 =
      s"""
         |SELECT
         |  ideaid,
         |  userid,
         |  unitid,
         |  adclass,
         |  media_type,
         |  media_appsid,
         |  adslotid,
         |  adslot_type,
         |  adtype,
         |  SUM(case when isclick=1 then price else 0 end) as total_price,
         |  SUM(isshow) as show_cnt,
         |  SUM(isclick) as ctr_cnt,
         |  SUM(case when isclick=1 then bid else 0 end) as total_bid,
         |  SUM(case when isclick=1 then exp_cvr else 0 end) as total_pcvr
         |FROM
         |  raw_table
         |GROUP BY ideaid, userid, unitid, adclass, media_type, media_appsid, adslotid, adslot_type, adtype
       """.stripMargin
    println(sqlRequest1)
    val data = spark.sql(sqlRequest1)

    val resultDF = data
      .select("ideaid", "userid", "unitid", "adclass", "media_type", "media_appsid", "adslotid", "adslot_type", "adtype", "total_price", "show_cnt", "ctr_cnt", "total_bid", "total_pcvr")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF

  }

}



