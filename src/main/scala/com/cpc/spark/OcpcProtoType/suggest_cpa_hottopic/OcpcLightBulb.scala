package com.cpc.spark.OcpcProtoType.suggest_cpa_hottopic

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.redis.RedisClient
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.OcpcProtoType.suggest_cpa_v1.OcpcLightBulb._
import org.apache.log4j.{Level, Logger}


object OcpcLightBulb{
  def main(args: Array[String]): Unit = {
    /*
    通过向redis中存储suggest cpa来控制灯泡的亮灯逻辑
    1. 抽取recommendation数据表
    2. mappartition打开redis，并存储数据
     */
    // 计算日期周期
//    2019-02-02 10 qtt_demo
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString


    val spark = SparkSession
      .builder()
      .appName(s"OcpcLightBulb: $date, $hour, $version")
      .enableHiveSupport().getOrCreate()


//    val tableName = "dl_cpc.ocpc_light_control_version"
    val tableName = "test.ocpc_qtt_light_control_version20190415"
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, tableName=$tableName")


    // 抽取数据
    val cpcData = getRecommendationAdV2(version, date, hour, spark)
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

//    resultDF
//      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_light_control_daily")
//
//    // 清除redis里面的数据
//    println(s"############## cleaning redis database ##########################")
//    cleanRedis(tableName, version, date, hour, spark)
//
//    // 存入redis
//    saveDataToRedis(version, date, hour, spark)
//    println(s"############## saving redis database ##########################")

    resultDF.repartition(5).write.mode("overwrite").saveAsTable(tableName)
//    resultDF.repartition(5).write.mode("overwrite").insertInto(tableName)
  }

  def getRecommendationAdV2(version: String, date: String, hour: String, spark: SparkSession) = {
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
         |and industry = 'feedapp'
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


}
