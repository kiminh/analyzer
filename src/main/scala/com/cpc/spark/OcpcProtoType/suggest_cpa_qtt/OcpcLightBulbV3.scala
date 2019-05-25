package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.OcpcProtoType.suggest_cpa_v1.OcpcLightBulbV3._


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
      .selectExpr("unit_id", "ocpc_light", "cast(round(current_cpa, 2) as double) as ocpc_suggest_price")
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))

    resultDF
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_light_api_control_daily")
//      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_light_api_control_daily")


  }


}
