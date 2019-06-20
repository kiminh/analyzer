package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt_hourly

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.OcpcProtoType.suggest_cpa_v2.OcpcLightBulbV3._
import org.apache.log4j.{Level, Logger}


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

    val result = getUpdateTableV2(date, hour, version, spark)
    val resultDF = result
      .withColumn("unit_id", col("unitid"))
      .selectExpr("unit_id", "ocpc_light", "cast(round(current_cpa, 2) as double) as ocpc_suggest_price")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
//      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_light_api_control_hourly")
      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_light_api_control_hourly")

//    // 清除redis里面的数据
//    println(s"############## cleaning redis database ##########################")
//    cleanRedis(version, date, hour, spark)
//
//    // 存入redis
//    saveDataToRedis(version, date, hour, spark)
//    println(s"############## saving redis database ################")


  }

  def getUpdateTableV2(date: String, hour: String, version: String, spark: SparkSession) = {
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

    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  cpa
         |FROM
         |  test.ocpc_light_control_prev_version
         |WHERE
         |  version = '$version'
       """.stripMargin

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

    // 数据关联
    val data = data2
      .join(data1, Seq("unitid", "conversion_goal"), "outer")
      .select("unitid", "conversion_goal", "current_cpa", "prev_cpa")
      .na.fill(-1, Seq("current_cpa", "prev_cpa"))
      .withColumn("ocpc_light", udfSetLightSwitch()(col("current_cpa"), col("prev_cpa")))

    //    data.write.mode("overwrite").saveAsTable("test.ocpc_check_data20190525")

    data

  }


}
