package com.cpc.spark.OcpcProtoType.model_v5

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


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
    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, hourInt:$hourInt, cvrType:$cvrType")

    OcpcSmoothFactor(date, hour, version, media, hourInt, cvrType, spark)
  }

  def OcpcSmoothFactor(date: String, hour: String, version: String, media: String, hourInt: Int, cvrType: String, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseData = getBaseData(media, cvrType, hourInt, date, hour, spark)

    // 计算结果
    val result = calculateSmooth(baseData, spark)

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
      .select("identifier", "click", "cv", "pre_cvr", "total_price", "total_bid")
      .withColumn("conversion_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(finalVersion))

    resultDF.show()

    resultDF
          .repartition(5).write.mode("overwrite").saveAsTable("test.check_cvr_smooth_data20190605")

    resultDF
  }



  def calculateSmooth(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .groupBy("unitid")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        avg(col("exp_cvr")).alias("pre_cvr"),
        sum(col("price")).alias("total_price"),
        sum(col("bid")).alias("total_bid")
      )
      .select("unitid", "click", "cv", "pre_cvr", "total_price", "total_bid")

    val result = data
        .selectExpr("cast(unitid as string) identifier", "click", "cv", "pre_cvr", "total_price", "total_bid")

    result
  }

  def getBaseData(media: String, cvrType: String, hourInt: Int, date: String, hour: String, spark: SparkSession) = {
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
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

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
         |  ocpc_log
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = '$cvrType'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid"), "left_outer")
      .select("searchid", "unitid", "isclick", "exp_cvr", "iscvr", "price", "bid")

    resultDF
  }


}