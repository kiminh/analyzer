package com.cpc.spark.OcpcProtoType.model_hottopic_hidden

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
    val media = args(2).toString
    val hourInt = args(3).toInt
    val cvrType = args(4).toString
    val version = args(5).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, hourInt:$hourInt, cvrType:$cvrType")

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
    val resultDF = result
        .select("identifier", "pcoc", "jfb", "post_cvr")
        .withColumn("conversion_goal", lit(conversionGoal))
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit(version))

    resultDF.show()

    resultDF
      .repartition(5).write.mode("overwrite").insertInto("dl_cpc.ocpc_pcoc_jfb_hourly")
//      .repartition(5).write.mode("overwrite").saveAsTable("test.check_cvr_smooth_data20190329")
  }



  def calculateSmooth(rawData: DataFrame, spark: SparkSession) = {
    val pcocData = calculatePCOC(rawData, spark)
    val jfbData = calculateJFB(rawData, spark)

    val result = pcocData
        .join(jfbData, Seq("unitid"), "outer")
        .selectExpr("cast(unitid as string) identifier", "pcoc", "jfb", "post_cvr")
        .filter(s"pcoc is not null and pcoc != 0")

    result
  }

  def calculateJFB(rawData: DataFrame, spark: SparkSession) = {
    val jfbData = rawData
      .groupBy("unitid")
      .agg(
        sum(col("price")).alias("total_price"),
        sum(col("bid")).alias("total_bid")
      )
      .select("unitid", "total_price", "total_bid")
      .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
      .select("unitid", "jfb")

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
      .select("unitid", "post_cvr", "pre_cvr")
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("unitid", "pcoc", "post_cvr")

    pcocData.show(10)

    pcocData
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