package com.cpc.spark.oCPX.oCPC.calibration

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcCVRfactor {
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
    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, hourInt:$hourInt")

    val result = OcpcSmoothFactorMain(date, hour, version, media, hourInt, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_smooth_factor20190702a")
  }

  def OcpcSmoothFactorMain(date: String, hour: String, version: String, media: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseData = getBaseData(media, hourInt, date, hour, spark)

    // 计算结果
    val result = calculateSmooth(baseData, spark)

    val finalVersion = version + hourInt.toString
    val resultDF = result
      .select("identifier", "conversion_goal", "click", "cv", "pre_cvr", "total_price", "total_bid", "hour_cnt")
      .filter(s"cv > 0")

    resultDF
  }



  def calculateSmooth(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .groupBy("unitid", "conversion_goal")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        avg(col("exp_cvr")).alias("pre_cvr"),
        sum(col("price")).alias("total_price"),
        sum(col("bid")).alias("total_bid"),
        countDistinct(col("hour")).alias("hour_cnt")
      )
      .select("unitid", "conversion_goal", "click", "cv", "pre_cvr", "total_price", "total_bid", "hour_cnt")

    val result = data
        .selectExpr("cast(unitid as string) identifier", "conversion_goal", "click", "cv", "pre_cvr", "total_price", "total_bid", "hour_cnt")

    result
  }

  def getBaseData(media: String, hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  isshow,
         |  isclick,
         |  bid_discounted_by_ad_slot as bid,
         |  price,
         |  cast(exp_cvr as double) as exp_cvr,
         |  conversion_goal,
         |  hour
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  isclick = 1
         |AND
         |  is_ocpc = 1
         |AND
         |  conversion_goal > 0
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .select("searchid", "unitid", "conversion_goal", "isclick", "exp_cvr", "iscvr", "price", "bid", "hour")

    resultDF
  }

}