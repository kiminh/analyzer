package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import ocpcminbidv2.ocpcminbidv2.{BidListV2, SingleBidv2}
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
    baseData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
      .repartition(50).write.mode("overwrite").insertInto("dl_cpc.ocpc_check_min_bid_base")
//      .repartition(50).write.mode("overwrite").saveAsTable("test.ocpc_check_min_bid_base")

    val resultDF = calculateMinBid(baseData, date, hour, spark)
    resultDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_check_min_bid")
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_check_min_bid")

    val data = resultDF.orderBy(desc("cnt")).limit(3000)

    savePbPack(data, "ocpc_minbidv2.pb")
  }

  def savePbPack(dataset: Dataset[Row], filename: String): Unit = {
    var list = new ListBuffer[SingleBidv2]
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
//      int32 hour = 1;
//      string adslotid = 2;
//      int32 cityLevel = 3;
//      int32 adsrc = 4;
//      int32 ad_second_class = 5;
//      int32 isOcpc = 6;
//      double minBid = 7;
//      hour,adslotid,city_level,,adsrc,ad_second_class,ocpc_flag
      val hour = record.getAs[String]("hr").toInt
      val adslotid = record.getAs[String]("adslotid")
      val city_level = record.getAs[Int]("city_level")
      // todo: adsrcc=2
      val adsrc = 2
      val ad_second_class = record.getAs[Long]("ad_second_class").toInt
      val ocpc_flag = record.getAs[Int]("ocpc_flag")
      val min_bid = record.getAs[Double]("min_bid")

      if (cnt % 100 == 0) {
        println(s"hour:$hour, adslotid:$adslotid, city_level:$city_level, adsrc:$adsrc, ad_second_class:$ad_second_class, ocpc_flag:$ocpc_flag, min_bid:$min_bid")
      }
      cnt += 1
      val currentItem = SingleBidv2(
        hour = hour,
        adslotid = adslotid,
        cityLevel = city_level,
        adsrc = adsrc,
        adSecondClass = ad_second_class,
        isOcpc = ocpc_flag,
        minBid = min_bid
      )
      list += currentItem

    }




    val result = list.toArray[SingleBidv2]
    val adRecordList = BidListV2(
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
         |  adslotid,
         |  city_level,
         |  adsrc,
         |  floor(adclass/1000) as ad_second_class,
         |  ocpc_flag,
         |  percentile(bid, 0.03) as min_bid,
         |  count(1) as cnt
         |FROM
         |  base_data
         |GROUP BY hr, adslotid, city_level, adsrc, floor(adclass/1000), ocpc_flag
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
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)
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