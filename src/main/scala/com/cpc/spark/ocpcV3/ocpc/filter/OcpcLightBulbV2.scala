package com.cpc.spark.ocpcV3.ocpc.filter

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.JSONObject
import com.cpc.spark.ocpc.OcpcUtils._
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object OcpcLightBulbV2{
  def main(args: Array[String]): Unit = {
    /*
    通过向redis中存储suggest cpa来控制灯泡的亮灯逻辑
    1. 从当天的结果表中抽取test.ocpc_qtt_light_control_data_redis
    2. 从mysql前端抽到每个unitid的conversion_goal
    3. 按照conversion_goal来抽取推荐cpa
    4.
     */
    // 计算日期周期
    //    2019-02-02 10 qtt_demo
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString


    val spark = SparkSession
      .builder()
      .appName(s"OcpcLightBulbV2: $date, $hour")
      .enableHiveSupport().getOrCreate()


    val tableName = "test.ocpc_qtt_light_control_v2"
//    val tableName = "test.ocpc_qtt_light_control_v2_20190314"

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, tableName=$tableName")

    // 抽取数据
    val completeData = getLightData(version, date, hour, spark)

    // 抽取转化目标
    val conversionGoal = getConversionGoal(date, hour, spark)

    // 按照conversion_goal来抽取推荐cpa
    val cpaSuggest = getCPAsuggest(completeData, conversionGoal, date, hour, spark)
    cpaSuggest.repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_qtt_light_control_20190305new")
//    cpaSuggest
//      .selectExpr("cast(unitid as string) unitid", "cast(conversion_goal as int) conversion_goal", "cpa")
//      .withColumn("date", lit(date))
//      .withColumn("version", lit("qtt_demo"))
//      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_qtt_light_control_v2")
//
//    // 清除redis数据
//    println(s"############## cleaning redis database ##########################")
//    cleanRedis(tableName, date, hour, spark)
//
//    // 存储redis数据
//    saveDataToRedis(date, hour, spark)
//    println(s"############## saving redis database ##########################")
//    // 存储结果表
//    cpaSuggest.repartition(5).write.mode("overwrite").saveAsTable(tableName)
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
          var key = "new_algorithm_unit_ocpc_" + identifier
          redis.del(key)
        }
      }
      redis.disconnect
    })
  }

  def saveDataToRedis(date: String, hour: String, spark: SparkSession) = {
    val data = spark
      .table("dl_cpc.ocpc_qtt_light_control_v2")
      .where(s"`date`='$date' and version='qtt_demo'")
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

//    for (record <- data.collect()) {
//      val identifier = record.getAs[Int]("unitid").toString
//      val valueDouble = record.getAs[Double]("cpa")
//      var key = "new_algorithm_unit_ocpc_" + identifier
//      if (valueDouble >= 0) {
//        var valueString = valueDouble.toString
//        if (valueString == "0.0") {
//          valueString = "0"
//        }
//        println(s"key:$key, value:$valueString")
//      }
//    }


    data.foreachPartition(iterator => {
      val redis = new RedisClient(host, port)
      redis.auth(auth)
      iterator.foreach{
        record => {
          val identifier = record.getAs[Int]("unitid").toString
          val valueDouble = record.getAs[Double]("cpa")
          var key = "new_algorithm_unit_ocpc_" + identifier
          if (valueDouble >= 0) {
            var valueString = valueDouble.toString
            if (valueString == "0.0") {
              valueString = "0"
            }
            println(s"key:$key, value:$valueString")
            redis.setex(key, 2 * 24 * 60 * 60, valueString)
          }
        }
      }
      redis.disconnect
    })
  }

  def getCPAsuggest(completeData: DataFrame, conversionGoal: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val result = conversionGoal
      .join(completeData, Seq("unitid", "conversion_goal"), "left_outer")
      .filter(s"cpa is not null")
      .select("unitid", "conversion_goal", "cpa")

    result.write.mode("overwrite").saveAsTable("test.ocpc_light_new_data20190304")
    val resultDF = result.filter("cpa >= 0")
    resultDF
  }

  def getConversionGoal(date: String, hour: String, spark: SparkSession) = {
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

    val resultDF = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .select("unitid",  "conversion_goal")

    resultDF.show(10)
    resultDF
  }


  def getLightData(version: String, date: String, hour: String, spark: SparkSession) = {
    val rawData = spark
      .table("test.ocpc_qtt_light_control_data_redis")
      .select("unitid", "cpa1", "cpa2", "cpa3")

    val data1 = rawData
      .withColumn("cpa", col("cpa1"))
      .withColumn("conversion_goal", lit(1))
      .select("unitid", "conversion_goal", "cpa")

    val data2 = rawData
      .withColumn("cpa", col("cpa2"))
      .withColumn("conversion_goal", lit(2))
      .select("unitid", "conversion_goal", "cpa")

    val data3 = rawData
      .withColumn("cpa", col("cpa3"))
      .withColumn("conversion_goal", lit(3))
      .select("unitid", "conversion_goal", "cpa")

    val data4 = getDataByConf(spark)

    val data = data1
      .union(data2)
      .union(data3)
      .withColumn("cpa1", col("cpa"))
      .select("unitid", "conversion_goal", "cp1")

    val result = data
      .join(data4, Seq("unitid", "conversion_goal"), "outer")
      .select("unitid", "conversion_goal", "cp1", "cpa2")
      .withColumn("cpa", when(col("cpa2").isNotNull, col("cpa2")).otherwise(col("cpa1")))

    result.show(10)
    result.write.mode("overwrite").saveAsTable("test.ocpc_light_new_data20190304a")

    val resultDF = result
        .select("unitid", "conversion_goal", "cp")

    resultDF
  }

  def getDataByConf(spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "ocpc_all.ocpc_reset_k"
    val expDataPath = conf.getString(conf_key)
    val rawData = spark.read.format("json").json(expDataPath)
    val data = rawData
      .filter(s"kvalue > 0")
      .select("identifier", "conversion_goal", "cpa")
      .groupBy("identifier", "conversion_goal")
      .agg(min(col("cpa")).alias("cpa2"))
      .select("identifier", "conversion_goal", "cpa2")
    data.show(10)
    data
  }

}

