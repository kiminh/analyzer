package com.cpc.spark.OcpcProtoType.suggest_cpa_v2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcLightBulbV3{
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

    val result = getUpdateTable(date, hour, version, spark)
    val resultDF = result
      .withColumn("unit_id", col("unitid"))
      .selectExpr("unit_id", "ocpc_light", "cast(round(current_cpa, 2) as double) as ocpc_suggest_price")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_light_api_control_hourly")
//      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_light_api_control_hourly")

    // 清除redis里面的数据
    println(s"############## cleaning redis database ##########################")
    cleanRedis(version, date, hour, spark)

    // 存入redis
    saveDataToRedis(version, date, hour, spark)
    println(s"############## saving redis database ################")


  }

  def getUpdateTable(date: String, hour: String, version: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -1)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)

    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  cpa
         |FROM
         |  dl_cpc.ocpc_light_control_prev_version
         |WHERE
         |  version = '$version'
       """.stripMargin

//    val sqlRequest1 =
//      s"""
//         |SELECT
//         |  unitid,
//         |  conversion_goal,
//         |  cpa
//         |FROM
//         |  dl_cpc.ocpc_light_control_hourly
//         |WHERE
//         |  `date` = '$date1'
//         |AND
//         |  `hour` = '$hour1'
//         |AND
//         |  version = '$version'
//       """.stripMargin

    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .groupBy("unitid", "conversion_goal")
      .agg(min(col("cpa")).alias("prev_cpa"))
      .select("unitid", "conversion_goal", "prev_cpa")
      .cache()

    data1.show(10)

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  cpa
         |FROM
         |  dl_cpc.ocpc_light_control_version
         |WHERE
         |  version = '$version'
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .groupBy("unitid", "conversion_goal")
      .agg(min(col("cpa")).alias("current_cpa"))
      .select("unitid", "conversion_goal", "current_cpa")
      .cache()
    data2.show(10)

    data2
      .withColumn("cpa", col("current_cpa"))
      .withColumn("version", lit(version))
      .select("unitid", "conversion_goal", "cpa", "version")
      .repartition(10)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_light_control_prev_version")

    // 数据关联
    val data = data2
      .join(data1, Seq("unitid", "conversion_goal"), "outer")
      .select("unitid", "conversion_goal", "current_cpa", "prev_cpa")
      .na.fill(-1, Seq("current_cpa", "prev_cpa"))
      .withColumn("ocpc_light", udfSetLightSwitch()(col("current_cpa"), col("prev_cpa")))

//    data.write.mode("overwrite").saveAsTable("test.ocpc_check_data20190525")

    data

  }

  def udfSetLightSwitch() = udf((currentCPA: Double, prevCPA: Double) => {
    var result = 1
    if (currentCPA >= 0) {
      result = 1
    } else {
      result = 0
    }
    result
  })


  def cleanRedis(version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    将对应key的值设成空的json字符串
     */
    val data = spark
      .table("dl_cpc.ocpc_light_api_control_hourly")
      .where(s"version = '$version' and `date` = '$date' and `hour` = '$hour' and ocpc_light = 0")
      .selectExpr("cast(unit_id as int) unitid", "cast(round(ocpc_suggest_price, 2) as double) as cpa")
      .repartition(2)
    //    val data = spark.table("test.ocpc_qtt_light_control_v2").repartition(2)
    data.show(10)
    val cnt = data.count()
    println(s"total size of the data is: $cnt")
    val conf = ConfigFactory.load("ocpc")
    val host = conf.getString("adv_redis.host")
    val port = conf.getInt("adv_redis.port")
    val auth = conf.getString("adv_redis.auth")
    println(s"host: $host")
    println(s"port: $port")

//    // 测试
//    for (record <- data.collect()) {
//      val identifier = record.getAs[Int]("unitid").toString
//      val valueDouble = record.getAs[Double]("cpa")
//      var key = "new_algorithm_unit_ocpc_" + identifier
//      println(s"key:$key")
//    }

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

//    // 测试
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
            redis.setex(key, 7 * 24 * 60 * 60, valueString)
          }
        }
      }
      redis.disconnect
    })
  }


}
