package com.cpc.spark.OcpcProtoType.suggest_cpa_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcSuggestCpaRecord {
  def main(args: Array[String]): Unit = {
    /*
    identifier维度下的累积最新版的kvalue和cpa_suggest
    
    1. 从当天的dl_cpc.ocpc_suggest_cpa_recommend_hourly表中抽取cpa与kvalue
    2. 读取最近72小时是否有ocpc广告记录，并加上flag
    3. 过滤出最近72小时没有ocpc广告记录的cpa与kvalue
    4. 读取前一天的时间分区中的所有cpa与kvalue
    5. 数据关联，并更新字段cpa，kvalue以及day_cnt字段
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version")

    // 从当天的dl_cpc.ocpc_suggest_cpa_recommend_hourly表中抽取cpa
    val suggestCPA = readCPAsuggest(version, date, hour, spark)
    suggestCPA.write.mode("overwrite").saveAsTable("test.check_ocpc_novel_record20190709a")

    // 读取最近72小时是否有ocpc广告记录，并加上flag
    val ocpcFlag = getOcpcFlag(date, hour, spark)

    // 过滤出最近72小时没有ocpc广告记录的cpa与kvalue
    val newData = getCleanData(suggestCPA, ocpcFlag, date, hour, spark)
    newData.write.mode("overwrite").saveAsTable("test.check_ocpc_novel_record20190709b")

    // 读取前一小时的时间分区中的所有cpa与kvalue
    val prevData = getPrevData(version, date, hour, spark)

    // 数据关联，并更新字段cpa，kvalue以及day_cnt字段
    val result = updateCPAsuggest(newData, prevData, spark)

    val resultDF = result
      .select("unitid", "userid", "adclass", "media", "conversion_goal", "cpa_suggest")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .cache()

    resultDF.show(10)

//    resultDF.repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_check_data20190704")
    resultDF
      .repartition(10)
//      .write.mode("overwrite").insertInto("test.ocpc_history_suggest_cpa_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_history_suggest_cpa_hourly")
    resultDF
      .repartition(10)
//      .write.mode("overwrite").insertInto("test.ocpc_history_suggest_cpa_version")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_history_suggest_cpa_version")


  }

  def getPrevData(version: String, date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"version = '$version'"
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  adclass,
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

    data.show(10)
    data
  }

  def getCleanData(suggestCPA: DataFrame, ocpcFlag: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val joinData = suggestCPA
      .join(ocpcFlag, Seq("unitid", "userid", "adclass", "media", "conversion_goal"), "left_outer")
      .select("unitid", "userid", "adclass", "media", "conversion_goal", "cpa_suggest", "click", "flag")
      .filter(s"flag is null")
      .filter("cpa_suggest is not null")

    joinData.show(10)
    joinData
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
         |    userid,
         |    adclass,
         |    isclick,
         |    (case
         |        when media_appsid in ('80000001', '80000002') then 'qtt'
         |        when media_appsid in ('80002819') then 'hottopic'
         |        else 'novel'
         |    end) as media,
         |    cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal,
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
      .groupBy("unitid", "userid", "adclass", "media", "conversion_goal")
      .agg(
        sum(col("isclick")).alias("click")
      )
      .withColumn("flag", lit(1))
      .select("unitid", "userid", "adclass", "media", "conversion_goal", "click", "flag")
      .filter(s"click>0")
    resultDF
  }

  def readCPAsuggest(version: String, date: String, hour: String, spark: SparkSession)= {
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  adclass,
         |  media,
         |  cpa as cpa_suggest,
         |  conversion_goal
         |FROM
         |  dl_cpc.ocpc_recommend_units_hourly
         |WHERE
         |  `date`='$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version='$version'
         |AND
         |  is_recommend = 1
       """.stripMargin
    println(sqlRequest1)
    val data = spark
      .sql(sqlRequest1)
      .groupBy("unitid", "userid", "adclass", "media", "conversion_goal")
      .agg(
        avg("cpa_suggest").alias("cpa_suggest")
      )
      .select("unitid", "userid", "adclass", "media", "conversion_goal", "cpa_suggest")



    data
  }

  def updateCPAsuggest(newDataRaw: DataFrame, prevDataRaw: DataFrame, spark: SparkSession) = {
    /*
    数据关联，并更新字段cpa，kvalue以及day_cnt字段
    1. 数据关联
    2. 是否有新的cpa、kvalue更新，
          如果有，更新cpa和kvalue，duration = 1
          如果没有，不更新cpa和kvalue，duration += 1

     newData: "identifier", "cpa_suggest", "kvalue", "conversion_goal"
     prevData: "identifier", "cpa_suggest", "kvalue", "conversion_goal", "duration"
     */
    val newData = newDataRaw
      .withColumn("new_cpa", col("cpa_suggest"))
      .select("unitid", "userid", "adclass", "media", "conversion_goal", "new_cpa")

    val prevData = prevDataRaw
      .withColumn("prev_cpa", col("cpa_suggest"))
      .select("unitid", "userid", "adclass", "media", "conversion_goal", "prev_cpa")


    // 以外关联的方式，将第三步得到的新表中的出价记录替换第四步中的对应的identifier的cpc出价，保存结果到新的时间分区
    val result = newData
      .join(prevData, Seq("unitid", "userid", "adclass", "media", "conversion_goal"), "outer")
      .select("unitid", "userid", "adclass", "media", "conversion_goal", "new_cpa", "prev_cpa")
      .withColumn("is_update", when(col("new_cpa").isNotNull, 1).otherwise(0))
      .withColumn("cpa_suggest", when(col("is_update") === 1, col("new_cpa")).otherwise(col("prev_cpa")))


    val resultDF = result.select("unitid", "userid", "adclass", "media", "conversion_goal", "cpa_suggest")
    resultDF

  }




}

