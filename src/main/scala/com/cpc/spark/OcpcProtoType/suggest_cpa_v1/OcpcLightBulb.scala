package com.cpc.spark.OcpcProtoType.suggest_cpa_v1

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSONObject
import com.cpc.spark.ocpc.OcpcUtils._
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable


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
    val media = args(3).toString


    val spark = SparkSession
      .builder()
      .appName(s"OcpcLightBulb: $date, $hour, $version")
      .enableHiveSupport().getOrCreate()


    val tableName = "dl_cpc.ocpc_light_control_version"
//    val tableName = "test.ocpc_qtt_light_control_version20190415"
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, tableName=$tableName")


    // 抽取数据
    val cpcData = getRecommendationAd(version, date, hour, spark)
    val ocpcData = getOcpcRecord(media, version, date, hour, spark)
    val cvUnit = getCPAgiven(date, hour, spark)


    val data = cpcData
        .join(ocpcData, Seq("unitid", "conversion_goal"), "outer")
        .select("unitid", "conversion_goal", "cpa1", "cpa2")
        .withColumn("cpa", when(col("cpa2").isNotNull && col("cpa2") >= 0, col("cpa2")).otherwise(col("cpa1")))
        .na.fill(-1, Seq("cpa1", "cpa2", "cpa"))

    data.show(10)

    val resultDF = data
        .join(cvUnit, Seq("unitid", "conversion_goal"), "inner")
        .select("unitid", "conversion_goal", "cpa")
        .selectExpr("cast(unitid as string) unitid", "conversion_goal", "cpa")
        .withColumn("date", lit(date))
        .withColumn("version", lit(version))

    resultDF.show(10)

    resultDF
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_light_control_daily")

    // 清除redis里面的数据
    println(s"############## cleaning redis database ##########################")
    cleanRedis(tableName, version, date, hour, spark)

    // 存入redis
    saveDataToRedis(version, date, hour, spark)
    println(s"############## saving redis database ##########################")

//    resultDF.repartition(5).write.mode("overwrite").saveAsTable(tableName)
    resultDF.repartition(5).write.mode("overwrite").insertInto(tableName)
  }

  def getOcpcRecord(media: String, version: String, date: String, hour: String, spark: SparkSession) = {
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

    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    val sqlRequest1 =
      s"""
         |SELECT
         |    unitid,
         |    cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    $selectCondition
         |AND
         |    $mediaSelection
         |AND
         |    is_ocpc = 1
         |AND
         |    isclick = 1
       """.stripMargin
    println(sqlRequest1)
    val rawData = spark.sql(sqlRequest1).distinct()

    val sqlRequets2 =
      s"""
         |SELECT
         |  cast(identifier as int) as unitid,
         |  conversion_goal,
         |  cpa_suggest * 1.0 / 100 as cpa2
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_k_version
         |WHERE
         |  version = '$version'
       """.stripMargin
    println(sqlRequets2)
    val suggestDataRaw = spark.sql(sqlRequets2)

    val result = rawData
        .join(suggestDataRaw, Seq("unitid", "conversion_goal"), "left_outer")
        .select("unitid", "conversion_goal", "cpa2")
        .na.fill(0, Seq("cpa2"))

    result.show(10)
    val resultDF = result.select("unitid", "conversion_goal", "cpa2")

    resultDF
  }

  def cleanRedis(tableName: String, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    将对应key的值设成空的json字符串
     */
    val data = spark.table(tableName).where(s"version = '$version'").repartition(2)
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
          var key = "new_algorithm_unit_ocpc_" + identifier
          redis.del(key)
        }
      }
      redis.disconnect
    })
  }

  def saveDataToRedis(version: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table("dl_cpc.ocpc_qtt_light_control_v2")
      .where(s"`date`='$date' and version='$version'")
      .repartition(2)

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

    for (record <- data.collect()) {
      val identifier = record.getAs[Int]("unitid").toString
      val valueDouble = record.getAs[Double]("cpa")
      var key = "new_algorithm_unit_ocpc_" + identifier
      if (valueDouble >= 0) {
        var valueString = valueDouble.toString
        if (valueString == "0.0") {
          valueString = "0"
        }
        println(s"key:$key, value:$valueString")
      }
    }


//    data.foreachPartition(iterator => {
//      val redis = new RedisClient(host, port)
//      redis.auth(auth)
//      iterator.foreach{
//        record => {
//          val identifier = record.getAs[Int]("unitid").toString
//          val valueDouble = record.getAs[Double]("cpa")
//          var key = "new_algorithm_unit_ocpc_" + identifier
//          if (valueDouble >= 0) {
//            var valueString = valueDouble.toString
//            if (valueString == "0.0") {
//              valueString = "0"
//            }
//            println(s"key:$key, value:$valueString")
//            redis.setex(key, 7 * 24 * 60 * 60, valueString)
//          }
//        }
//      }
//      redis.disconnect
//    })
  }

  def getRecommendationAd(version: String, date: String, hour: String, spark: SparkSession) = {
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
         |SELECT
         |    unitid,
         |    conversion_goal,
         |    cpa * 1.0 / 100 as cpa1
         |FROM
         |    dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |    date = '$date'
         |AND
         |    `hour` = '06'
         |and is_recommend = 1
         |and version = '$version'
         |and industry in ('elds', 'feedapp')
       """.stripMargin

//    val sqlRequest =
//        s"""
//           |select
//           |    a.unitid,
//           |	    a.original_conversion as conversion_goal,
//           |    a.cpa / 100.0 as cpa1
//           |FROM
//           |    (SELECT
//           |        *
//           |    FROM
//           |        dl_cpc.ocpc_suggest_cpa_recommend_hourly
//           |    WHERE
//           |        date = '$date'
//           |    AND
//           |        `hour` = '06'
//           |    and is_recommend = 1
//           |    and version = '$version'
//           |    and industry in ('elds', 'feedapp')) as a
//           |INNER JOIN
//           |    (
//           |        select distinct unitid, adslot_type
//           |        FROM dl_cpc.ocpc_ctr_data_hourly
//           |        where date >= '$date1'
//           |    ) as b
//           |ON
//           |    a.unitid=b.unitid
//         """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF.show(10)
    resultDF
  }

  def getCPAgiven(date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._
    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select id, user_id, ideas, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal, status from adv.unit where ideas is not null) as tmp"

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
      .select("unitid", "conversion_goal")


    base.createOrReplaceTempView("base_table")

    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    cast(conversion_goal as int) as conversion_goal
         |FROM
         |    base_table
       """.stripMargin

    println(sqlRequest)

    val resultDF = spark.sql(sqlRequest).distinct()

    resultDF.show(10)
    resultDF


  }

}
