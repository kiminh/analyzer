package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import mincpm.Mincpm
import mincpm.mincpm.{MinCpmList, SingleMinCpm}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object OcpcMinBidV3 {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcMinBid: $date, $hour").enableHiveSupport().getOrCreate()

    // 抽取数据
    val baseData1 = getBaseData(date, hour, 7, spark)

    // 抽取expctr
    val expCtrData = getPreCtr(date, hour, 7, spark)

    val baseData = baseData1
      .join(expCtrData, Seq("searchid"), "inner")
      .select("searchid", "ideaid", "original_bid", "price", "bid", "ocpc_flag", "is_ocpc", "isshow", "isclick", "ocpc_log", "adslotid", "adslot_type", "user_city", "city_level", "adsrc", "adclass", "hr", "exp_ctr")
      .withColumn("cpm", col("bid") * col("exp_ctr"))

    baseData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
//      .repartition(50).write.mode("overwrite").insertInto("dl_cpc.ocpc_check_min_bid_base_v2")
      .repartition(50).write.mode("overwrite").saveAsTable("test.ocpc_check_min_bid_base")

    val result = calculateMinBid(baseData, date, hour, spark)

//    hr,
//    adslot_type,
//    city_level,
//    floor(adclass/1000) as ad_second_class,
//    ocpc_flag,
//    percentile(bid, 0.03) as min_bid,
//    percentile(cpm, 0.03) as min_cpm,
//    count(1) as cnt
    val resultDF = result
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))

    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_check_min_bid_v2")

    resultDF.repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_check_min_bid")

    val data = resultDF.filter(s"cnt>=300")

    savePbPack(data, "ad_mincpm.pb")
  }

  def getPreCtr(date: String, hour: String, dayInt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayInt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = getTimeRangeSql3(date1, hour, date, hour)
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  exp_ctr
         |FROM
         |  dl_cpc.slim_union_log
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF
  }

  def savePbPack(dataset: Dataset[Row], filename: String): Unit = {
    var list = new ListBuffer[SingleMinCpm]
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
//      int32 hour = 1;
//      int64 adslotType = 2;
//      int32 cityLevel = 3;
//      int32 adSecondClass = 4;
//      int32 isOcpc = 5;
//      double minBid = 6;
//      int64 minCpm = 7;
      val hr = record.getAs[String]("hr").toInt
      val adslot_type = record.getAs[Long]("adslot_type")
      val city_level = record.getAs[Int]("city_level")
      val ad_second_class = record.getAs[Long]("ad_second_class").toInt
      val ocpc_flag = record.getAs[Int]("ocpc_flag")
      val min_bid = record.getAs[Double]("min_bid")
      val min_cpm = record.getAs[Long]("min_cpm")

      if (cnt % 100 == 0) {
        println(s"hour:$hr, adslot_type:$adslot_type, city_level:$city_level, ad_second_class:$ad_second_class, ocpc_flag:$ocpc_flag, min_bid:$min_bid, min_cpm:$min_cpm")
      }
      cnt += 1
      val currentItem = SingleMinCpm(
        hour = hr,
        adslotType = adslot_type,
        cityLevel = city_level,
        adSecondClass = ad_second_class,
        isOcpc = ocpc_flag,
        minBid = min_bid,
        minCpm = min_cpm
      )
      list += currentItem

    }

    val result = list.toArray[SingleMinCpm]
    val adRecordList = MinCpmList(
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
    //    hour,adslotid,city_level,,adsrc,ad_second_class,ocpc_flag
    val sqlRequest =
    s"""
       |SELECT
       |  hr,
       |  adslot_type,
       |  city_level,
       |  floor(adclass/1000) as ad_second_class,
       |  ocpc_flag,
       |  percentile(bid, 0.03) as min_bid,
       |  percentile(cpm, 0.06) as min_cpm,
       |  count(1) as cnt
       |FROM
       |  base_data
       |GROUP BY hr, adslot_type, city_level, floor(adclass/1000), ocpc_flag
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

    val resultDF = rawData
      .withColumn("min_cnt", lit(minCnt))
      .selectExpr("hr", "cast(adslot_type as bigint) adslot_type", "city_level", "ad_second_class", "ocpc_flag", "min_bid", "cast(min_cpm as bigint) min_cpm", "cnt", "min_cnt")


    resultDF
  }

  def getBaseData(date: String, hour: String, dayInt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayInt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)
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
       |    hour as hr,
       |    adslotid,
       |    adslot_type,
       |    user_city,
       |    city_level,
       |    adsrc,
       |    adclass
       |from dl_cpc.filtered_union_log_hourly
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
         |    hr
         |from ctr_data
       """.stripMargin
    println(sqlRequest2)
    val data = spark.sql(sqlRequest2)
    val resultDF = data
      .selectExpr("searchid", "ideaid", "original_bid", "price", "cast(bid as int) as bid", "ocpc_flag", "is_ocpc", "isshow", "isclick", "ocpc_log", "adslotid", "adslot_type", "user_city", "city_level", "adsrc", "adclass", "hr")

    resultDF
  }

}
