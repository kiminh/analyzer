package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt_hourly

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcLightBulbV4{
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
      .appName(s"OcpcLightBulbV4: $date, $hour, $version")
      .enableHiveSupport().getOrCreate()

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -3)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)


    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  cpa
         |FROM
         |  dl_cpc.ocpc_light_control_hourly
         |WHERE
         |  `date` = '$date1'
         |AND
         |  `hour` = '$hour1'
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .groupBy("unitid", "conversion_goal")
      .agg(min(col("cpa")).alias("current_cpa"))
      .select("unitid", "conversion_goal", "current_cpa")
      .cache()
    data.show(10)

    data
      .withColumn("cpa", col("current_cpa"))
      .withColumn("version", lit(version))
      .select("unitid", "conversion_goal", "cpa", "version")
      .repartition(10)
      .write.mode("overwrite").saveAsTable("test.ocpc_light_control_prev_version")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_light_control_prev_version")


  }


}
