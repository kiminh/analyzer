package com.cpc.spark.OcpcProtoType.suggest_cpa_v3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcUpdatePrevLight{
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
      .appName(s"OcpcUpdatePrevLight: $date, $hour, $version")
      .enableHiveSupport().getOrCreate()


    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  adclass,
         |  media,
         |  cpa
         |FROM
         |  dl_cpc.ocpc_unit_light_control_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .groupBy("unitid", "userid", "adclass", "media")
      .agg(
        min(col("cpa")).alias("current_cpa")
      )
      .select("unitid", "userid", "adclass", "media", "current_cpa")
      .cache()
    data.show(10)

    data
      .withColumn("cpa", col("current_cpa"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("unitid", "userid", "adclass", "media", "cpa", "date", "hour", "version")
      .repartition(10)
      .write.mode("overwrite").insertInto("test.ocpc_unit_light_control_prev_version")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_unit_light_control_prev_version")


  }


}
