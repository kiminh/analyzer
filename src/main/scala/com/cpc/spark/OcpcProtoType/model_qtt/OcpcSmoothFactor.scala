package com.cpc.spark.OcpcProtoType.model_qtt

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.model_v4.OcpcSmoothFactor._


object OcpcSmoothFactor{
  def main(args: Array[String]): Unit = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val hourInt = args(4).toInt
    val cvrType = args(5).toString
    val minCV = args(6).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, hourInt:$hourInt, cvrType:$cvrType, minCV:$minCV")

    val baseData = getBaseData(media, cvrType, hourInt, date, hour, spark)

    // 计算结果
    val result = calculateSmooth(baseData, minCV, spark)

    var conversionGoal = 1
    if (cvrType == "cvr1") {
      conversionGoal = 1
    } else if (cvrType == "cvr2") {
      conversionGoal = 2
    } else {
      conversionGoal = 3
    }

    val finalVersion = version + hourInt.toString
    val resultDF = result
        .select("identifier", "pcoc", "jfb", "post_cvr")
        .withColumn("conversion_goal", lit(conversionGoal))
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit(finalVersion))

    resultDF.show()

    resultDF
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_pcoc_jfb_hourly")
//      .repartition(5).write.mode("overwrite").saveAsTable("test.check_cvr_smooth_data20190329")
  }

}