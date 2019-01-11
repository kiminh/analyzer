package com.cpc.spark.ocpcV3.ocpc.filter

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col


object OcpcMinBid {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcMinBid: $date, $hour").enableHiveSupport().getOrCreate()

    // 抽取数据
    val baseData = getBaseData(date, hour, spark)
    baseData.repartition(50).write.mode("overwrite").saveAsTable("test.ocpc_check_min_bid_base")
//    val baseData = spark.table("test.ocpc_check_min_bid_base")

    val resultDF = calculateMinBid(baseData, date, hour, spark)
    resultDF.repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_check_min_bid")


  }

  def calculateMinBid(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    data.createOrReplaceTempView("base_data")
//    hour,adslotid, user_city,city_level, adtype,adsrc,adclass,is_ocpc
    val sqlRequest =
      s"""
         |SELECT
         |  hour,
         |  adslotid,
         |  adslot_type,
         |  user_city,
         |  city_level,
         |  adsrc,
         |  adclass,
         |  ocpc_flag,
         |  percentile(bid, 0.05) as min_bid
         |FROM
         |  base_data
         |WHERE
         |  ocpc_flag=1
         |GROUP BY hour, adslotid, adslot_type, user_city, city_level, adsrc, adclass, ocpc_flag
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getBaseData(date: String, hour: String, spark: SparkSession) = {
    // 取历史区间: score数据
//    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
//    val today = dateConverter.parse(date)
//    val calendar = Calendar.getInstance
//    calendar.setTime(today)
//    calendar.add(Calendar.DATE, -1)
//    val yesterday = calendar.getTime
//    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date`='$date' and `hour` <= '$hour'"
    // todo 时间区间： hour
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    ideaid,
         |    bid as original_bid,
         |    isshow,
         |    isclick,
         |    price,
         |    ext_int['is_ocpc'] as is_ocpc,
         |    ext_string['ocpc_log'] as ocpc_log,
         |    hour,
         |    adslotid,
         |    adslot_type,
         |    ext_string['user_city'] as user_city,
         |    ext['city_level'].int_value as city_level,
         |    adsrc,
         |    ext['adclass'].int_value as adclass
         |from dl_cpc.cpc_union_log
         |where $selectCondition
         |and `hour` <= '08'
         |and isshow = 1
         |and ext['exp_ctr'].int_value is not null
         |and media_appsid  in ("80000001", "80000002")
         |and ext['antispam'].int_value = 0
         |and ideaid > 0 and adsrc = 1
         |and ext_int['dsp_adnum_by_src_1'] > 1
         |and userid > 0
         |and (ext['charge_type'] IS NULL OR ext['charge_type'].int_value = 1)
       """.stripMargin
    println(sqlRequest)
    val ctrData = spark
      .sql(sqlRequest)
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    ctrData.createOrReplaceTempView("ctr_data")
    val sqlRequest2 =
      s"""
         |select
         |    searchid,
         |    ideaid,
         |    original_bid,
         |    isshow,
         |    isclick,
         |    price,
         |    ocpc_log,
         |    is_ocpc,
         |    (case when length(ocpc_log) > 0 then ocpc_log_dict['dynamicbid']
         |          else original_bid end) as bid,
         |    (case when length(ocpc_log) > 0 and is_ocpc=1 then 1
         |          else 0 end) as ocpc_flag,
         |    adslotid,
         |    adslot_type,
         |    user_city,
         |    city_level,
         |    adsrc,
         |    adclass
         |from ctr_data
       """.stripMargin
    println(sqlRequest2)
    val data = spark.sql(sqlRequest2)
    val resultDF = data
      .selectExpr("searchid", "ideaid", "original_bid", "price", "cast(bid as int) as bid", "ocpc_flag", "is_ocpc", "isshow", "isclick", "ocpc_log", "adslotid", "adslot_type", "user_city", "city_level", "adsrc", "adclass", "hour")

    resultDF
  }

}