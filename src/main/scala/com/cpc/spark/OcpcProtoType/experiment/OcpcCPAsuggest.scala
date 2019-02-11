package com.cpc.spark.OcpcProtoType.experiment

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcCPAsuggest {
  def main(args: Array[String]): Unit = {
    /*
    identifier维度下的cpa_suggest实验：按照推荐cpa对计算的ecpm进行降权

    ecpm_new = ecpm * (cpa_given / cpa_suggest)^t
    此处，当cpa_given <= 0.75 * cpa_suggest时，t=3；否则t=0。

    推荐cpa从dl_cpc.ocpc_suggest_cpa_recommend_hourly这张表里面读取

    1. 读取今日的推荐cpa
    2. 和历史推荐cpa作比较，更新到最新的时间分区中
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    val version = args(3).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media")

    // 读取今日的推荐cpa
    val suggestCPA = readCPAsuggest(version, date, hour, spark)

    // 和历史推荐cpa作比较，更新到最新的时间分区中
    val resultDF = updateCPAsuggest(media, version, suggestCPA, date, hour, spark)
    resultDF.repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_check_data20190211")


  }

  def readCPAsuggest(version: String, date: String, hour: String, spark: SparkSession)= {
    val sqlRequest1 =
      s"""
         |SELECT
         |  cast(unitid as string) as identifier,
         |  cpa as cpa_suggest,
         |  original_conversion as conversion_goal
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |  `date`='$date'
         |AND
         |  `hour`='$hour'
         |AND
         |  version='$version'
       """.stripMargin
    println(sqlRequest1)
    val data = spark.sql(sqlRequest1)

    data
  }

  def getDurationByDay(date: String, hour: String, dayInt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayInt)
    val startdate = calendar.getTime
    val date1 = dateConverter.format(startdate)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)

    selectCondition
  }

  def updateCPAsuggest(media: String, version: String, cpaSuggest: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    1. 获取推荐cpa
    2. 检查最近七天是否有ocpc展现记录
    3. 过滤掉最近七天有ocpc展现记录的广告，保留剩下的推荐cpa
    4. 读取前一天的推荐cpa数据表
    5. 以外关联的方式，将第三步得到的新表中的出价记录替换第四步中的对应的identifier的推荐cpa，保存结果到新的时间分区
     */
    val conf_key = "medias." + media + ".media_selection"
    val conf = ConfigFactory.load("ocpc")
    val mediaSelection = conf.getString(conf_key)
    // 检查最近七天是否有ocpc展现记录
    val selectCondition1 = getDurationByDay(date, hour, 7, spark)
    val sqlRequets1 =
      s"""
         |SELECT
         |  searchid,
         |  cast(unitid as string) as identifier
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition1
         |AND
         |  is_ocpc=1
         |AND
         |  $mediaSelection
       """.stripMargin
    println(sqlRequets1)
    val ocpcRecord = spark
      .sql(sqlRequets1)
      .withColumn("is_ocpc", lit(1))
      .select("identifier", "is_ocpc")
      .distinct()

    // 过滤掉最近七天有ocpc展现记录的广告，保留剩下的推荐cpa
    val joinData = cpaSuggest
      .join(ocpcRecord, Seq("identifier"), "left_outer")
      .select("identifier", "cpa_suggest", "conversion_goal", "is_ocpc")
      .na.fill(0, Seq("is_ocpc"))

    val currentCPA = joinData
      .filter(s"is_ocpc=0")
      .withColumn("current_cpa", col("cpa_suggest"))
      .select("identifier", "current_cpa", "conversion_goal")


    // 读取前一天的推荐cpa数据表
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val startdate = calendar.getTime
    val date1 = dateConverter.format(startdate)
    val selectCondition2 = s"`date` = '$date1' and version = '$version'"
    val sqlRequest2 =
      s"""
         |SELECT
         |  identifier,
         |  cpc_suggest as prev_cpa,
         |  conversion_goal,
         |  duration as prev_duration
         |FROM
         |  dl_cpc.ocpc_cpc_cpa_exp
         |WHERE
         |  $selectCondition2
       """.stripMargin
    println(sqlRequest2)
    val prevCPA = spark.sql(sqlRequest2)

    // 以外关联的方式，将第三步得到的新表中的出价记录替换第四步中的对应的identifier的cpc出价，保存结果到新的时间分区
    val result = prevCPA
      .join(currentCPA, Seq("identifier", "conversion_goal"), "outer")
      .select("identifier", "conversion_goal", "current_cpa", "prev_cpa", "prev_duration")
      .withColumn("is_update", when(col("current_cpa").isNotNull, 1).otherwise(0))
      .withColumn("cpa_suggest", when(col("is_update") === 1, col("current_cpa")).otherwise(col("prev_cpa")))
      .withColumn("duration", when(col("is_update") === 1, 1).otherwise(col("prev_duration") + 1))

    val resultDF = result.select("identifier", "conversion_goal", "cpa_suggest", "duration")
    resultDF

  }




}

