package com.cpc.spark.OcpcProtoType.suggest_cpa_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools.getConfCPA
import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcSuggestCpaRecordV2 {
  def main(args: Array[String]): Unit = {
    /*
    identifier维度下的累积最新版的kvalue和cpa_suggest
    
    1. 从当天的dl_cpc.ocpc_suggest_cpa_recommend_hourly表中抽取cpa与kvalue
    2. 读取最近72小时是否有ocpc广告记录，并加上flag
    3. 过滤出最近72小时没有ocpc广告记录的cpa与kvalue
    4. 读取前一天的时间分区中的所有cpa与kvalue
    5. 数据关联，并更新字段cpa，kvalue以及day_cnt字段
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version")

    // 从当天的dl_cpc.ocpc_suggest_cpa_recommend_hourly表中抽取cpa
    val suggestCPA = readCPAsuggest(version, date, hour, spark)

    // 读取前一小时的时间分区中的所有cpa与kvalue
    val prevData = getPrevData(version, date, hour, spark)

    // 数据关联，并更新字段cpa，kvalue以及day_cnt字段
    val result = updateCPAsuggest(suggestCPA, prevData, spark)

    val resultDF = result
      .select("unitid", "media", "conversion_goal", "cpa_suggest")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .repartition(50)
      .cache()

    resultDF.show(10)


    resultDF
      .write.mode("overwrite").insertInto("test.ocpc_history_suggest_cpa_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_history_suggest_cpa_hourly")
    resultDF
      .write.mode("overwrite").insertInto("test.ocpc_history_suggest_cpa_version")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_history_suggest_cpa_version")


  }

  def getPrevData(version: String, date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"version = '$version'"
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  media,
         |  conversion_goal,
         |  cpa_suggest
         |FROM
         |  dl_cpc.ocpc_history_suggest_cpa_version
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val data = spark
        .sql(sqlRequest)
        .filter(s"cpa_suggest is not null")
        .cache()

    data.show(10)
    data
  }

  def getCleanData(suggestCPA: DataFrame, ocpcFlag: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val joinData = suggestCPA
      .join(ocpcFlag, Seq("unitid", "media", "conversion_goal"), "left_outer")
//      .select("unitid", "media", "conversion_goal", "cpa_suggest", "flag")
      .select("unitid", "media", "conversion_goal", "cpa_suggest", "click", "flag")

    val result = joinData
      .filter(s"flag is null")
      .filter("cpa_suggest is not null")

    result
  }

  def getOcpcFlag(date: String, hour: String, spark: SparkSession) = {
    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -3)
    val yesterday1 = calendar.getTime
    val date1 = dateConverter.format(yesterday1)
    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    isclick,
         |    (case
         |        when media_appsid in ('80000001', '80000002') then 'qtt'
         |        when media_appsid in ('80002819', '80004944', '80004948') then 'hottopic'
         |        else 'novel'
         |    end) as media,
         |    conversion_goal,
         |    cast(ocpc_log_dict['IsHiddenOcpc'] as int) as is_hidden
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    $selectCondition
         |and is_ocpc=1
         |and $mediaSelection
         |and round(adclass/1000) != 132101  --去掉互动导流
         |and isclick = 1
         |and ideaid > 0
         |and adslot_type in (1,2,3)
         |and searchid is not null
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .filter(s"is_hidden is null or is_hidden = 0")
//      .select("unitid", "media", "conversion_goal")
//      .distinct()
      .groupBy("unitid", "media", "conversion_goal")
      .agg(
        sum(col("isclick")).alias("click")
      )
      .withColumn("flag", lit(1))
//      .select("unitid", "media", "conversion_goal", "flag")
      .select("unitid", "media", "conversion_goal", "click", "flag")
      .filter(s"click>0")
    resultDF
  }

  def readCPAsuggest(version: String, date: String, hour: String, spark: SparkSession)= {
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  media,
         |  cpa as cpa1,
         |  conversion_goal,
         |  is_recommend
         |FROM
         |  dl_cpc.ocpc_recommend_units_hourly
         |WHERE
         |  `date`='$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version='$version'
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .groupBy("unitid", "media", "conversion_goal", "is_recommend")
      .agg(
        avg("cpa1").alias("cpa1")
      )
      .select("unitid", "media", "conversion_goal", "cpa1", "is_recommend")


    val data2raw = getConfCPA(version, date, hour, spark)
    val data2 = data2raw
      .withColumn("cpa2", col("cpa_suggest"))
      .select("unitid", "media", "cpa2")


    val data = data1
        .join(data2, Seq("unitid", "media"), "left_outer")
        .select("unitid", "media", "conversion_goal", "cpa1", "cpa2", "is_recommend")
        .withColumn("is_recommend", when(col("cpa2").isNotNull, 1).otherwise(col("is_recommend")))
        .withColumn("cpa_suggest", when(col("cpa2").isNotNull, col("cpa2")).otherwise(col("cpa1")))

    val resultDF = data
        .filter(s"is_recommend = 1")
        .select("unitid", "media", "conversion_goal", "cpa_suggest")
        .cache()

    resultDF.show(10)

    resultDF
  }

  def updateCPAsuggest(suggestDataRaw: DataFrame, prevDataRaw: DataFrame, spark: SparkSession) = {
    /*
    数据关联，并更新字段cpa，kvalue以及day_cnt字段
    1. 数据关联
    2. 是否有新的cpa、kvalue更新，
          如果有，更新cpa和kvalue，duration = 1
          如果没有，不更新cpa和kvalue，duration += 1

     newData: "identifier", "cpa_suggest", "kvalue", "conversion_goal"
     prevData: "identifier", "cpa_suggest", "kvalue", "conversion_goal", "duration"
     */
    val suggestData = suggestDataRaw
      .withColumn("new_cpa", col("cpa_suggest"))
      .select("unitid", "media", "conversion_goal", "new_cpa")

    val prevData = prevDataRaw
      .withColumn("prev_cpa", col("cpa_suggest"))
      .select("unitid", "media", "conversion_goal", "prev_cpa")


    // 以外关联的方式，将第三步得到的新表中的出价记录替换第四步中的对应的identifier的cpc出价，保存结果到新的时间分区
    val result = suggestData
      .join(prevData, Seq("unitid", "media", "conversion_goal"), "outer")
      .select("unitid", "media", "conversion_goal", "new_cpa", "prev_cpa")
      .withColumn("is_update", when(col("prev_cpa").isNotNull, 0).otherwise(1))
      .withColumn("cpa_suggest", when(col("is_update") === 1, col("new_cpa")).otherwise(col("prev_cpa")))


    val resultDF = result.select("unitid", "media", "conversion_goal", "cpa_suggest")
    resultDF

  }




}

