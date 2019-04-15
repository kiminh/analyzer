package com.cpc.spark.OcpcProtoType.suggest_cpa_v1

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcSuggestKcpa {
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
    val media = args(3).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media")

    // 从当天的dl_cpc.ocpc_suggest_cpa_recommend_hourly表中抽取cpa与kvalue
    val suggestCPA = readCPAsuggest(version, date, hour, spark)

    // 读取最近72小时是否有ocpc广告记录，并加上flag
    val ocpcFlag = getOcpcFlag(media, date, hour, spark)

    // 过滤出最近72小时没有ocpc广告记录的cpa与kvalue
    val newData = getCleanData(suggestCPA, ocpcFlag, date, hour, spark)

    // 读取前一天的时间分区中的所有cpa与kvalue
    val prevData = getPrevData(version, date, hour, spark)

    // 数据关联，并更新字段cpa，kvalue以及day_cnt字段
    val result = updateCPAsuggest(newData, prevData, spark)

    val resultDF = result
      // kvalue表示刚进入ocpc时的cpc阶段算出来的k值，
      // duration表示进入ocpc的天数
      // 要获取刚进入ocpc阶段的信息时，这张表常用
      .select("identifier", "cpa_suggest", "kvalue", "conversion_goal", "duration")
      .withColumn("date", lit(date))
      .withColumn("version", lit(version))

//    resultDF.repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_check_data20190301")
//    resultDF.repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_k")
    resultDF.repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_k_version")


  }

  def getPrevData(version: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday1 = calendar.getTime
    val date1 = dateConverter.format(yesterday1)
    val selectCondition = s"`date` = '$date1' and version = '$version'"

    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  cpa_suggest,
         |  kvalue,
         |  conversion_goal,
         |  duration
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_k
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val data = spark
        .sql(sqlRequest)
        .filter(s"cpa_suggest is not null and kvalue is not null")

    data.show(10)
    data
  }

  def getCleanData(suggestCPA: DataFrame, ocpcFlag: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val joinData = suggestCPA
      .join(ocpcFlag, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "cpa_suggest", "kvalue", "conversion_goal", "click", "flag")
      .filter(s"flag is null")
      .filter("cpa_suggest is not null and kvalue is not null")

    joinData.show(10)
    joinData
  }

  def getOcpcFlag(media: String, date: String, hour: String, spark: SparkSession) = {
    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
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
         |    isclick,
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
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and searchid is not null
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .filter(s"is_hidden is null or is_hidden = 0")
      .groupBy("unitid", "conversion_goal")
      .agg(sum(col("isclick")).alias("click"))
      .withColumn("identifier", col("unitid"))
      .withColumn("flag", lit(1))
      .select("identifier", "conversion_goal", "click", "flag")
      .filter(s"click>0")

    resultDF.show(10)
    resultDF
  }

  def readCPAsuggest(version: String, date: String, hour: String, spark: SparkSession)= {
    val sqlRequest1 =
      s"""
         |SELECT
         |  cast(unitid as string) as identifier,
         |  cpa as cpa_suggest,
         |  kvalue as kvalue,
         |  original_conversion as conversion_goal
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |  `date`='$date'
         |AND
         |  `hour` = '06'
         |AND
         |  version='$version'
         |AND
         |  is_recommend = 1
       """.stripMargin
    println(sqlRequest1)
    val data = spark
      .sql(sqlRequest1)
      .groupBy("identifier", "conversion_goal")
      .agg(
        avg("cpa_suggest").alias("cpa_suggest"),
        avg("kvalue").alias("kvalue")
      )
      .select("identifier", "cpa_suggest", "kvalue", "conversion_goal")



    data
  }

//  def getDurationByDay(date: String, hour: String, dayInt: Int, spark: SparkSession) = {
//    // 取历史数据
//    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
//    val today = dateConverter.parse(date)
//    val calendar = Calendar.getInstance
//    calendar.setTime(today)
//    calendar.add(Calendar.DATE, -dayInt)
//    val startdate = calendar.getTime
//    val date1 = dateConverter.format(startdate)
//    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)
//
//    selectCondition
//  }

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
      .withColumn("new_k", col("kvalue"))
      .select("identifier", "new_cpa", "new_k", "conversion_goal")

    val prevData = prevDataRaw
      .withColumn("prev_cpa", col("cpa_suggest"))
      .withColumn("prev_k", col("kvalue"))
      .withColumn("prev_duration", col("duration"))
      .select("identifier", "prev_cpa", "prev_k", "conversion_goal", "prev_duration")


    // 以外关联的方式，将第三步得到的新表中的出价记录替换第四步中的对应的identifier的cpc出价，保存结果到新的时间分区
    val result = newData
      .join(prevData, Seq("identifier", "conversion_goal"), "outer")
      .select("identifier", "conversion_goal", "new_cpa", "prev_cpa", "prev_duration", "new_k", "prev_k")
      .withColumn("is_update", when(col("new_cpa").isNotNull, 1).otherwise(0))
      .withColumn("cpa_suggest", when(col("is_update") === 1, col("new_cpa")).otherwise(col("prev_cpa")))
      .withColumn("kvalue", when(col("is_update") === 1, col("new_k")).otherwise(col("prev_k")))
      .withColumn("duration", when(col("is_update") === 1, 1).otherwise(col("prev_duration") + 1))


    val resultDF = result.select("identifier", "cpa_suggest", "kvalue", "conversion_goal", "duration")
    resultDF

  }




}

