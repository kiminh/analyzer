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
//    2019-02-02 10 qtt_demo
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString


    val spark = SparkSession
      .builder()
      .appName(s"OcpcLightBulb: $date, $hour")
      .enableHiveSupport().getOrCreate()


    val tableName = "test.ocpc_qtt_light_control20190305"

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, tableName=$tableName")


    // 抽取数据
    val cpcData = getRecommendationAd(date, hour, spark)
    val ocpcData = getOcpcRecord(date, hour, spark)
    val ocpcUnit = getCPAgiven(date, hour, spark)
    val ocpcRecord = ocpcData.join(ocpcUnit, Seq("unitid"), "inner").select("unitid", "ocpc_cpa1", "ocpc_cpa2", "ocpc_cpa3")

    val data = cpcData
        .join(ocpcRecord, Seq("unitid"), "outer")
        .select("unitid", "cpc_cpa1", "cpc_cpa2", "cpc_cpa3", "ocpc_cpa1", "ocpc_cpa2", "ocpc_cpa3")
        .na.fill(-1, Seq("cpc_cpa1", "cpc_cpa2", "cpc_cpa3", "ocpc_cpa1", "ocpc_cpa2", "ocpc_cpa3"))

//    data
//      .withColumn("date", lit(date))
//      .withColumn("version", lit("qtt_demo"))
//      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_qtt_light_control")

//    // 清除redis里面的数据
//    println(s"############## cleaning redis database ##########################")
//    cleanRedis(tableName, date, hour, spark)
//
//    // 存入redis
//    saveDataToRedis(date, hour, spark)
//    println(s"############## saving redis database ##########################")

    data.repartition(5).write.mode("overwrite").saveAsTable(tableName)
  }

  def getOcpcRecord(date: String, hour: String, spark: SparkSession) = {
    /*
    抽取最近三天所有广告单元的投放记录
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -3)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    timestamp,
         |    cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
         |    cast(ocpc_log_dict['cpagiven'] as double) * 1.0 / 100 as cpa_given,
         |    row_number() over(partition by unitid order by timestamp desc) as seq
         |FROM
         |    dl_cpc.ocpc_union_log_hourly
         |WHERE
         |    $selectCondition
         |AND
         |    media_appsid  in ("80000001", "80000002")
         |AND
         |    ext_int['is_ocpc'] = 1
         |AND
         |    isclick = 1
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)

    val data = rawData
      .filter(s"seq=1")
      .select("unitid", "conversion_goal", "cpa_given")

    val data1 = data.filter(s"conversion_goal=1").withColumn("cpa_given1", col("cpa_given")).select("unitid", "cpa_given1")
    val data2 = data.filter(s"conversion_goal=2").withColumn("cpa_given2", col("cpa_given")).select("unitid", "cpa_given2")
    val data3 = data.filter(s"conversion_goal=3").withColumn("cpa_given3", col("cpa_given")).select("unitid", "cpa_given3")

    val cpaGivenData = data1
        .join(data2, Seq("unitid"), "outer")
        .join(data3, Seq("unitid"), "outer")
        .select("unitid", "cpa_given1", "cpa_given2", "cpa_given3")
        .na.fill(-1, Seq("cpa_given1", "cpa_given2", "cpa_given3"))

    val sqlRequets1 =
      s"""
         |SELECT
         |  identifier as unitid,
         |  conversion_goal,
         |  cpa_suggest * 1.0 / 100 as cpa_suggest
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_k_once
         |WHERE
         |  duration <= 3
       """.stripMargin
    println(sqlRequets1)
    val suggestDataRaw = spark.sql(sqlRequets1)
    val suggestData1 = suggestDataRaw
      .filter(s"conversion_goal=1")
      .withColumn("cpa_suggest1", col("cpa_suggest"))
      .select("unitid", "cpa_suggest1")
    val suggestData2 = suggestDataRaw
      .filter(s"conversion_goal=2")
      .withColumn("cpa_suggest2", col("cpa_suggest"))
      .select("unitid", "cpa_suggest2")
    val suggestData3 = suggestDataRaw
      .filter(s"conversion_goal=3")
      .withColumn("cpa_suggest3", col("cpa_suggest"))
      .select("unitid", "cpa_suggest3")
    val suggestData = suggestData1
        .join(suggestData2, Seq("unitid"), "outer")
        .join(suggestData3, Seq("unitid"), "outer")
        .select("unitid", "cpa_suggest1", "cpa_suggest2", "cpa_suggest3")
        .na.fill(-1, Seq("cpa_suggest1", "cpa_suggest2", "cpa_suggest3"))

    val result = cpaGivenData
        .join(suggestData, Seq("unitid"), "left_outer")
        .select("unitid", "cpa_given1", "cpa_given2", "cpa_given3", "cpa_suggest1", "cpa_suggest2", "cpa_suggest3")
        .withColumn("ocpc_cpa1", when(col("cpa_given1") === -1, -1).otherwise(when(col("cpa_suggest1") === -1, 0).otherwise(col("cpa_suggest1"))))
        .withColumn("ocpc_cpa2", when(col("cpa_given2") === -1, -1).otherwise(when(col("cpa_suggest2") === -1, 0).otherwise(col("cpa_suggest2"))))
        .withColumn("ocpc_cpa3", when(col("cpa_given3") === -1, -1).otherwise(when(col("cpa_suggest3") === -1, 0).otherwise(col("cpa_suggest3"))))

    result.write.mode("overwrite").saveAsTable("test.check_data_ocpc_20190305")
    result.show(10)
    val resultDF = result.select("unitid", "ocpc_cpa1", "ocpc_cpa2", "ocpc_cpa3")

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
          redis.del(key)
        }
      }
      redis.disconnect
    })
  }

  def saveDataToRedis(date: String, hour: String, spark: SparkSession) = {
    val rawData = spark
      .table("dl_cpc.ocpc_qtt_light_control")
      .where(s"`date`='$date' and version='qtt_demo'")
      .repartition(2)

    val data = rawData
        .withColumn("cpa1", when(col("ocpc_cpa1") === -1, col("cpc_cpa1")).otherwise(col("ocpc_cpa1")))
        .withColumn("cpa2", when(col("ocpc_cpa2") === -1, col("cpc_cpa2")).otherwise(col("ocpc_cpa2")))
        .withColumn("cpa3", when(col("ocpc_cpa3") === -1, col("cpc_cpa3")).otherwise(col("ocpc_cpa3")))
        .selectExpr("unitid", "cpc_cpa1", "cpc_cpa2", "cpc_cpa3", "ocpc_cpa1", "ocpc_cpa2", "ocpc_cpa3", "cast(round(cpa1, 2) as double) as cpa1", "cast(round(cpa2, 2) as double) as cpa2", "cast(round(cpa3, 2) as double) as cpa3")
    data.write.mode("overwrite").saveAsTable("test.ocpc_qtt_light_control_data_redis")
    data.show(10)
    data.printSchema()
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
          val cpa1 = record.getAs[Double]("cpa1")
          val cpa2 = record.getAs[Double]("cpa2")
          val cpa3 = record.getAs[Double]("cpa3")
          println(s"cpa1:$cpa1, cpa2:$cpa2, cpa3:$cpa3")
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
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

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
           |    and industry in ('elds', 'feedapp')) as a
           |INNER JOIN
           |    (
           |        select distinct unitid, adslot_type
           |        FROM dl_cpc.ocpc_ctr_data_hourly
           |        where date >= '$date1'
           |    ) as b
           |ON
           |    a.unitid=b.unitid
         """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    val data1 = data.filter(s"conversion_goal=1").withColumn("cpc_cpa1", col("cpa")).select("unitid", "cpc_cpa1")
    val data2 = data.filter(s"conversion_goal=2").withColumn("cpc_cpa2", col("cpa")).select("unitid", "cpc_cpa2")
    val data3 = data.filter(s"conversion_goal=3").withColumn("cpc_cpa3", col("cpa")).select("unitid", "cpc_cpa3")

    val resultDF = data1
      .join(data2, Seq("unitid"), "outer")
      .join(data3, Seq("unitid"), "outer")
      .select("unitid", "cpc_cpa1", "cpc_cpa2", "cpc_cpa3")
      .na.fill(-1.0, Seq("cpc_cpa1", "cpc_cpa2", "cpc_cpa3"))

    resultDF.show(10)
    resultDF
  }

  def getCPAgiven(date: String, hour: String, spark: SparkSession) = {
    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select id, user_id, ideas, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal, status from adv.unit where is_ocpc=1 and ideas is not null) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val base = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .select("unitid", "userid", "ocpc_bid", "ocpc_bid_update_time", "conversion_goal", "status")


    base.createOrReplaceTempView("base_table")

    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    conversion_goal
         |    status
         |FROM
         |    base_table
       """.stripMargin

    println(sqlRequest)

    val resultDF = spark.sql(sqlRequest).select("unitid").distinct()

    resultDF.show(10)
    resultDF


  }

}
