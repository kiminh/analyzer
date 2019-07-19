package com.cpc.spark.OcpcProtoType.suggest_cpa_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.redis.RedisClient
import org.apache.spark.sql.DataFrame
//import com.cpc.spark.ocpc.OcpcUtils._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcLightBulbBackup{
  def main(args: Array[String]): Unit = {
    /*
    通过向redis中存储suggest cpa来控制灯泡的亮灯逻辑
    1. 抽取recommendation数据表
    2. mappartition打开redis，并存储数据
     */
    // 计算日期周期
//    2019-02-02 10 qtt_demo qtt
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString


    val spark = SparkSession
      .builder()
      .appName(s"OcpcLightBulbV2: $date, $hour, $version")
      .enableHiveSupport().getOrCreate()

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version")

    // 获取当前灯泡数据
    val data = getLightUpdate(version, date, hour, spark)
    val resultDF = data
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))


    resultDF
      .repartition(5)
//      .write.mode("overwrite").insertInto("test.ocpc_light_api_control_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_light_api_control_hourly")




    // 清除redis里面的数据
    println(s"############## cleaning redis database ##########################")
    cleanRedis(version, date, hour, spark)

    // 存入redis
    saveDataToRedis(version, date, hour, spark)
    println(s"############## saving redis database ################")
  }

  def cleanRedis(version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    将对应key的值设成空的json字符串
     */
    val data = spark
      .table("dl_cpc.ocpc_light_api_control_hourly")
      .where(s"version = '$version' and `date` = '$date' and `hour` = '$hour' and ocpc_light = 0")
      .selectExpr("cast(unit_id as int) unitid", "cast(round(ocpc_suggest_price, 2) as double) as cpa")
      .repartition(2)
    data.show(10)
    val cnt = data.count()
    println(s"total size of the data is: $cnt")
    val conf = ConfigFactory.load("ocpc")
    val host = conf.getString("adv_redis.host")
    val port = conf.getInt("adv_redis.port")
    val auth = conf.getString("adv_redis.auth")
    println(s"host: $host")
    println(s"port: $port")

    // 测试
    for (record <- data.collect()) {
      val identifier = record.getAs[Int]("unitid").toString
      val valueDouble = record.getAs[Double]("cpa")
      var key = "new_algorithm_unit_ocpc_" + identifier
      println(s"key:$key")
    }

//    data.foreachPartition(iterator => {
//      val redis = new RedisClient(host, port)
//      redis.auth(auth)
//      iterator.foreach{
//        record => {
//          val identifier = record.getAs[Int]("unitid").toString
//          var key = "new_algorithm_unit_ocpc_" + identifier
//          redis.del(key)
//        }
//      }
//      redis.disconnect
//    })
  }

  def saveDataToRedis(version: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table("dl_cpc.ocpc_light_api_control_hourly")
      .where(s"version = '$version' and `date` = '$date' and `hour` = '$hour' and ocpc_light = 1")
      .selectExpr("cast(unit_id as int) unitid", "cast(round(ocpc_suggest_price, 2) as double) as cpa")
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

    // 测试
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

  def getLightUpdate(version: String, date: String, hour: String, spark: SparkSession) = {
    // 抽取备份数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid as unit_id,
         |  ocpc_light
         |FROM
         |  dl_cpc.ocpc_adv_light_status_hourly
         |WHERE
         |  date = '$date'
         |AND
         |  hour = '$hour'
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    // 抽取推荐cpa
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  media,
         |  conversion_goal,
         |  cpa_suggest * 1.0 / 100 as cpa
         |FROM
         |  dl_cpc.ocpc_history_suggest_cpa_version
         |WHERE
         |  version = '$version'
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .select("unitid", "media", "conversion_goal", "cpa")
      .groupBy("unitid")
      .agg(
        avg(col("cpa")).alias("cpa")
      )
      .selectExpr("cast(unitid as int) unit_id", "cpa")

    // 数据关联
    val data = data1
      .join(data2, Seq("unit_id"), "left_outer")
      .na.fill(0.0, Seq("cpa"))
      .withColumn("cpa", when(col("ocpc_light") === 0, -1.0).otherwise(col("cpa")))
      .selectExpr("unit_id", "ocpc_light", "cast(round(cpa, 2) as double) as ocpc_suggest_price")
      .cache()

    data


  }

}
