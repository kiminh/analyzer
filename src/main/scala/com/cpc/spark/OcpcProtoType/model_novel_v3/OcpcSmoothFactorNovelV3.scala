package com.cpc.spark.OcpcProtoType.model_novel_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.model_novel_v3.OcpcSuggestCPAV3.matchcvr
import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcSmoothFactorNovelV3{
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
    val media = args(2).toString
    val hourInt = args(3).toInt
    val version = args(4).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, hourInt:$hourInt")

    val baseData = getBaseData(media, hourInt, date, hour, spark)

    // 计算结果
    val result = calculateSmooth(baseData, spark)

    val resultDF = result
        .select("identifier", "pcoc", "jfb", "post_cvr")
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit(version))

    resultDF.show()

    resultDF
      .repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_pcoc_jfb_novel_v3_hourly")
//      .repartition(5).write.mode("overwrite").saveAsTable("test.check_cvr_smooth_data20190329")
  }



  def calculateSmooth(rawData: DataFrame, spark: SparkSession) = {
    val pcocData = calculatePCOC(rawData, spark)
    val jfbData = calculateJFB(rawData, spark)

    val result = pcocData
        .join(jfbData, Seq("unitid"), "outer")
        .selectExpr("cast(unitid as string) identifier", "pcoc", "jfb", "post_cvr")

    result.show(10)

    result
  }

  def calculateJFB(rawData: DataFrame, spark: SparkSession) = {
    val jfbData = rawData
      .groupBy("unitid")
      .agg(
        sum(col("price")).alias("total_price"),
        sum(col("bid")).alias("total_bid")
      )
      .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
      .select("unitid", "total_price","jfb")

    jfbData.show()

    jfbData
  }


  def calculatePCOC(rawData: DataFrame, spark: SparkSession) = {
    val pcocData = rawData
      .groupBy("unitid")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        avg(col("exp_cvr")).alias("pre_cvr")
      )
      .select("unitid", "click", "cv", "pre_cvr")
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("unitid", "pcoc", "post_cvr")

    pcocData.show(10)

    pcocData
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
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)
    val selectCondition2 = getTimeRangeSql4(date1, hour1, date, hour)
    val selectCondition3 = s"day between '$date1' and '$date'"

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  isshow,
         |  isclick,
         |  bid as original_bid,
         |  price,
         |  exp_cvr,
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
    val base = spark
      .sql(sqlRequest)
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    base.createOrReplaceTempView("base_table")
    val sqlRequestBase =
      s"""
         |select
         |    searchid,
         |    unitid,
         |    price,
         |    original_bid,
         |    cast(exp_cvr as double) as exp_cvr,
         |    isclick,
         |    isshow,
         |    ocpc_log,
         |    ocpc_log_dict,
         |    (case when length(ocpc_log)>0 then cast(cast(ocpc_log_dict['dynamicbid'] as double) + 0.5 as int) else original_bid end) as bid
         |from base_table
       """.stripMargin
    println(sqlRequestBase)
    val clickData = spark.sql(sqlRequestBase)
    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |select distinct a.searchid,
         |       a.conversion_target as unit_target,
         |       b.conversion_target[0] as real_target
         |from
         |   (select *
         |    from dl_cpc.dm_conversions_for_model
         |   where $selectCondition2
         |and size(conversion_target)>0) a
         |join dl_cpc.dw_unitid_detail b
         |    on a.unitid=b.unitid
         |    and a.day = b.day
         |    and b.$selectCondition3
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2)
      .withColumn("iscvr",matchcvr(col("unit_target"),col("real_target")))
      .filter("iscvr = 1")

    // 数据关联
    val resultDF = clickData
      .join(cvrData, Seq("searchid"), "left_outer")
      .select("searchid", "unitid","isclick", "exp_cvr", "iscvr", "price", "bid")

    resultDF.show(10)

    resultDF
  }
}