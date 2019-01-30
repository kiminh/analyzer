package com.cpc.spark.ocpcV3.ocpc.filter

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSONObject
import com.cpc.spark.ocpc.OcpcUtils._
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object OcpcLightBulb{
  def main(args: Array[String]): Unit = {
    /*
    通过向redis中存储suggest cpa来控制灯泡的亮灯逻辑
    1. 抽取recommendation数据表
    2. mappartition打开redis，并存储数据
     */
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = "qtt_demo"
    val spark = SparkSession
      .builder()
      .appName(s"OcpcLightBulb: $date, $hour")
      .enableHiveSupport().getOrCreate()

    // 清楚redis里面的数据
//    cleanRedis("test.ocpc_qtt_light_control", date, hour, spark)


    // 抽取数据
    val cpcData = getRecommendationAd(date, hour, spark)
    val ocpcData = getOcpcRecord(date, hour, spark)
    val data = cpcData
        .join(ocpcData, Seq("unitid"), "outer")
        .select("unitid", "cpa1", "cpa2", "cpa3", "ocpc_cpa1", "ocpc_cpa2", "ocpc_cpa3")
        .na.fill(-1, Seq("cpa1", "cpa2", "cpa3", "ocpc_cpa1", "ocpc_cpa2", "ocpc_cpa3"))
    data.repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_qtt_light_control20190130")

    // 存入redis
//    saveDataToRedis("test.ocpc_qtt_light_control", date, hour, spark)
  }

  def getOcpcRecord(date: String, hour: String, spark: SparkSession) = {
    /*
    抽取最近七天所有广告单元的投放记录
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -7)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
//    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)
    val selectCondition = s"`dt`>='$date1'"

    val sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    timestamp,
         |    cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
         |    cast(ocpc_log_dict['cpagiven'] as double) as cpa_given,
         |    row_number() over(partition by unitid order by timestamp desc) as seq
         |FROM
         |    dl_cpc.ocpc_union_log_hourly
         |WHERE
         |    $selectCondition
         |AND
         |    media_appsid  in ("80000001", "80000002")
         |AND
         |    ext_int['is_ocpc'] = 1
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)

    val data = rawData
      .filter(s"seq=1")
      .select("unitid", "conversion_goal", "cpa_given")

    val data1 = data.filter(s"conversion_goal=1").withColumn("ocpc_cpa1", col("cpa_given")).select("unitid", "ocpc_cpa1")
    val data2 = data.filter(s"conversion_goal=2").withColumn("ocpc_cpa2", col("cpa_given")).select("unitid", "ocpc_cpa2")
    val data3 = data.filter(s"conversion_goal=3").withColumn("ocpc_cpa3", col("cpa_given")).select("unitid", "ocpc_cpa3")

    val resultDF = data1
        .join(data2, Seq("unitid"), "outer")
        .join(data3, Seq("unitid"), "outer")
        .select("unitid", "ocpc_cpa1", "ocpc_cpa2", "ocpc_cpa3")
        .na.fill(-1, Seq("ocpc_cpa1", "ocpc_cpa2", "ocpc_cpa3"))

    resultDF
  }

  def cleanRedis(tableName: String, date: String, hour: String, spark: SparkSession) = {
    /*
    将对应key的值设成空的json字符串
     */
    val data = spark.table(tableName).repartition(2)
    data.show(10)
    val cnt = data.count()
    println(s"total size of the data is: $cnt")
    val conf = ConfigFactory.load("ocpc")
    val host = conf.getString("adv_redis.host")
    val port = conf.getInt("adv_redis.port")
    val auth = conf.getString("adv_redis.auth")
    println(s"host: $host")
    println(s"port: $port")

    data.foreachPartition(iterator => {
      val redis = new RedisClient(host, port)
      redis.auth(auth)
      iterator.foreach{
        record => {
          val identifier = record.getAs[Int]("unitid").toString
          var key = "algorithm_unit_ocpc_" + identifier
          val json = new JSONObject()
          val value = json.toString
          redis.setex(key, 1 * 24 * 60 * 60, value)
        }
      }
      redis.disconnect
    })
  }

  def saveDataToRedis(tableName: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.table(tableName).repartition(2)
    data.show(10)
    val cnt = data.count()
    println(s"total size of the data is: $cnt")
    val conf = ConfigFactory.load("ocpc")
    val host = conf.getString("adv_redis.host")
    val port = conf.getInt("adv_redis.port")
    val auth = conf.getString("adv_redis.auth")
    println(s"host: $host")
    println(s"port: $port")
//    println(s"auth: $auth")
//
//    val redis = new RedisClient(host, port)
//    redis.auth(auth)
//    for (t <- 1 to 5) {
//      val identifier = "000000" + t.toString
//      val cpa1 = 1.0
//      val cpa2 = 1.0
//      val cpa3 = 1.0
//      var key = "algorithm_unit_ocpc_" + identifier
//      val json = new JSONObject()
//      if (cpa1 > 0) {
//        json.put("download_cpa", cpa1)
//      }
//      if (cpa2 > 0) {
//        json.put("appact_cpa", cpa2)
//      }
//      if (cpa3 > 0) {
//        json.put("formsubmit_cpa", cpa3)
//      }
//      val value = json.toString
//      println(s"key=$key, value=$value")
//      redis.setex(key, 2 * 24 * 60 * 60, value)
//    }
//    redis.disconnect

//    for (record <- data.collect()) {
//      val identifier = record.getAs[Int]("unitid").toString
//      val cpa1 = record.getAs[Double]("cpa1")
//      val cpa2 = record.getAs[Double]("cpa2")
//      val cpa3 = record.getAs[Double]("cpa3")
//      var key = "algorithm_unit_ocpc_" + identifier
//      val json = new JSONObject()
//      if (cpa1 > 0) {
//        json.put("download_cpa", cpa1)
//      }
//      if (cpa2 > 0) {
//        json.put("appact_cpa", cpa2)
//      }
//      if (cpa3 > 0) {
//        json.put("formsubmit_cpa", cpa3)
//      }
//      val value = json.toString
//      if (cpa2 > 0 || cpa3 > 0) {
//        println(s"key=$key, value=$value")
//      }
//
//    }

    data.foreachPartition(iterator => {
      val redis = new RedisClient(host, port)
      redis.auth(auth)
      iterator.foreach{
        record => {
          val identifier = record.getAs[Int]("unitid").toString
          val cpa1 = record.getAs[Double]("cpa1")
          val cpa2 = record.getAs[Double]("cpa2")
          val cpa3 = record.getAs[Double]("cpa3")
          var key = "algorithm_unit_ocpc_" + identifier
          val json = new JSONObject()
          if (cpa1 > 0) {
            json.put("download_cpa", cpa1)
          }
          if (cpa2 > 0) {
            json.put("appact_cpa", cpa2)
          }
          if (cpa3 > 0) {
            json.put("formsubmit_cpa", cpa3)
          }
          val value = json.toString
          redis.setex(key, 2 * 24 * 60 * 60, value)
        }
      }
      redis.disconnect
    })
  }

  def getRecommendationAd(date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date`='$date' and `hour`='$hour' and version='qtt_demo'"
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  unitid,
//         |  original_conversion as conversion_goal,
//         |  cpa * 1.0 / 100 as cpa
//         |FROM
//         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly
//         |WHERE
//         |  $selectCondition
//         |AND
//         |  is_recommend=1
//       """.stripMargin
    val sqlRequest =
        s"""
           |select
           |    a.unitid,
           |	    a.original_conversion as conversion_goal,
           |    a.cpa / 100.0 as cpa
           |FROM
           |    (SELECT
           |        *
           |    FROM
           |        dl_cpc.ocpc_suggest_cpa_recommend_hourly
           |    WHERE
           |        date = '$date'
           |    and is_recommend = 1
           |    and version = 'qtt_demo'
           |    and industry in ('elds')) as a
           |INNER JOIN
           |    (
           |        select distinct unitid, adslot_type
           |        FROM dl_cpc.ocpc_ctr_data_hourly
           |        where date = '$date'
           |    ) as b
           |ON
           |    a.unitid=b.unitid
         """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    val data1 = data.filter(s"conversion_goal=1").withColumn("cpa1", col("cpa")).select("unitid", "cpa1")
    val data2 = data.filter(s"conversion_goal=2").withColumn("cpa2", col("cpa")).select("unitid", "cpa2")
    val data3 = data.filter(s"conversion_goal=3").withColumn("cpa3", col("cpa")).select("unitid", "cpa3")

    val resultDF = data1
      .join(data2, Seq("unitid"), "outer")
      .join(data3, Seq("unitid"), "outer")
      .select("unitid", "cpa1", "cpa2", "cpa3")
      .na.fill(-1.0, Seq("cpa1", "cpa2", "cpa3"))

    resultDF.show(10)
    resultDF
  }


}
