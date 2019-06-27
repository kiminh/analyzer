package com.cpc.spark.OcpcProtoType.suggest_cpa_novel_hourly

import com.cpc.spark.OcpcProtoType.suggest_cpa_v2.OcpcLightBulbV3._
import org.apache.log4j.{Level, Logger}
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
    Logger.getRootLogger.setLevel(Level.WARN)
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


}
