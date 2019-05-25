package com.cpc.spark.OcpcProtoType.suggest_cpa_v1

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.httpclient.HttpClient
import org.apache.commons.httpclient.methods._




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

    val result = getUpdateTable(date, version, spark)
    val resultDF = result
      .withColumn("unit_id", col("unitid"))
      .withColumn("ocpc_suggest_price", col("currentCPA"))
      .select("unit_id", "ocpc_light", "ocpc_suggest_price")
      .withColumn("version", lit(version))


  }

  def getUpdateTable(date: String, version: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  cpa
         |FROM
         |  dl_cpc.ocpc_light_control_daily
         |WHERE
         |  `date` = '$date1'
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .groupBy("unitid", "conversion_goal")
      .agg(min(col("cpa")).alias("prev_cpa"))
      .select("unitid", "conversion_goal", "prev_cpa")

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

    // 数据关联
    val data = data2
      .join(data1, Seq("unitid", "conversion_goal"), "outer")
      .select("unitid", "conversion_goal", "current_cpa", "prev_cpa")
      .na.fill(-1, Seq("current_cpa", "prev_cpa"))
      .withColumn("ocpc_light", udfSetLightSwitch()(col("current_cpa"), col("prev_cpa")))

    data.write.mode("overwrite").saveAsTable("test.ocpc_check_data20190525")

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



}
