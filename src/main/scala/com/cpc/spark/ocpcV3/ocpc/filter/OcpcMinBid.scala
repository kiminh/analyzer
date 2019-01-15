package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import ocpcminbidv1.Ocpcminbidv1
import ocpcminbidv1.ocpcminbidv1.{BidList, SingleRecord}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


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
//    val resultDF = spark.table("test.ocpc_check_min_bid")
    val data = resultDF.filter(s"cnt >= min_cnt")

    savePbPack(data, "test_minbid.pb")
  }

  def savePbPack(dataset: Dataset[Row], filename: String): Unit = {
    var list = new ListBuffer[SingleRecord]
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
//      int32 hour = 1;
//      string adslotid = 2;
//      int32 adslotType = 3;
//      string userCity = 4;
//      int32 cityLevel = 5;
//      int32 adsrc = 6;
//      int32 adclass = 7;
//      int32 isOcpc = 8;
//      double minBid = 9;

      //hour    string  NULL
      //adslotid        string  NULL
      //adslot_type     int     NULL
      //user_city       string  NULL
      //city_level      int     NULL
      //adsrc   int     NULL
      //adclass int     NULL
      //ocpc_flag       int     NULL
      //min_bid double  NULL
      val hour = record.getAs[String]("hour").toInt
      val adslotid = record.getAs[String]("adslotid")
      val adslot_type = record.getAs[Int]("adslot_type")
      val user_city = record.getAs[String]("user_city")
      val city_level = record.getAs[Int]("city_level")
      val adsrc = record.getAs[Int]("adsrc")
      val adclass = record.getAs[Int]("adclass")
      val ocpc_flag = record.getAs[Int]("ocpc_flag")
      val min_bid = record.getAs[Double]("min_bid")

      if (cnt % 100 == 0) {
        println(s"hour:$hour, adslotid:$adslotid, adslot_type:$adslot_type, user_city:$user_city, city_level:$city_level, adsrc:$adsrc, adclass:$adclass, ocpc_flag:$ocpc_flag, min_bid:$min_bid")
      }
      cnt += 1
      val currentItem = SingleRecord(
        hour = hour,
        adslotid = adslotid,
        adslotType = adslot_type,
        userCity = user_city,
        cityLevel = city_level,
        adsrc = adsrc,
        adclass = adclass,
        isOcpc = ocpc_flag,
        minBid = min_bid
      )
      list += currentItem

    }
    val result = list.toArray[SingleRecord]
    val adRecordList = BidList(
      adrecord = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

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
         |  percentile(bid, 0.03) as min_bid,
         |  count(1) as cnt
         |FROM
         |  base_data
         |WHERE
         |  ocpc_flag=1
         |GROUP BY hour, adslotid, adslot_type, user_city, city_level, adsrc, adclass, ocpc_flag
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)
    rawData.createOrReplaceTempView("raw_data")

    val sqlRequest2 =
      s"""
         |SELECT
         |  percentile(cnt, 0.75) as min_cnt
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest2)
    val cntData = spark.sql(sqlRequest2)
    cntData.show(10)
    val minCnt = cntData.first().getAs[Double]("min_cnt")
    println(s"minCnt is $minCnt")

    val resultDF = rawData.withColumn("min_cnt", lit(minCnt))


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
    val selectCondition = s"`dt`='$date' and `hour` <= '$hour'"
    // todo 时间区间： hour
//    val sqlRequest =
//      s"""
//         |select
//         |    searchid,
//         |    ideaid,
//         |    bid as original_bid,
//         |    isshow,
//         |    isclick,
//         |    price,
//         |    ext_int['is_ocpc'] as is_ocpc,
//         |    ext_string['ocpc_log'] as ocpc_log,
//         |    hour,
//         |    adslotid,
//         |    adslot_type,
//         |    ext_string['user_city'] as user_city,
//         |    ext['city_level'].int_value as city_level,
//         |    adsrc,
//         |    ext['adclass'].int_value as adclass
//         |from test.filtered_union_log_hourly
//         |where $selectCondition
//         |and ext['exp_ctr'].int_value is not null
//         |and media_appsid  in ("80000001", "80000002")
//         |and ideaid > 0 and adsrc = 1
//         |and userid > 0
//         |and (ext['charge_type'] IS NULL OR ext['charge_type'].int_value = 1)
//       """.stripMargin
    val sqlRequest =
    s"""
       |select
       |    searchid,
       |    ideaid,
       |    bid as original_bid,
       |    isshow,
       |    isclick,
       |    price,
       |    is_ocpc,
       |    ocpc_log,
       |    hour,
       |    adslotid,
       |    adslot_type,
       |    user_city,
       |    city_level,
       |    adsrc,
       |    adclass
       |from test.filtered_union_log_hourly
       |where $selectCondition
       |and media_appsid  in ("80000001", "80000002")
       |and ideaid > 0 and adsrc = 1
       |and userid > 0
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
         |    adclass,
         |    hour
         |from ctr_data
       """.stripMargin
    println(sqlRequest2)
    val data = spark.sql(sqlRequest2)
    val resultDF = data
      .selectExpr("searchid", "ideaid", "original_bid", "price", "cast(bid as int) as bid", "ocpc_flag", "is_ocpc", "isshow", "isclick", "ocpc_log", "adslotid", "adslot_type", "user_city", "city_level", "adsrc", "adclass", "hour")

    resultDF
  }

}