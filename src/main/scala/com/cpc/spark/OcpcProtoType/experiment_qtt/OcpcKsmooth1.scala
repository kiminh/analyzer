package com.cpc.spark.OcpcProtoType.experiment_qtt

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import ocpc.Ocpc
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import ocpcabtest.ocpcabtest.{OcpcList, SingleRecord}

import scala.collection.mutable.ListBuffer

object OcpcKsmooth1 {
  def main(args: Array[String]): Unit = {
    /*
    k值平滑策略：
    根据从推荐cpa里面读取的kvalue，以及推荐日期，计算推荐日期与当前时间的小时级差值，按照小时级差值计算合理阈值
    1. 抽取前24小时是否有ocpc广告记录，生成flag
    2. 抽取最近一次的推荐cpa程序，拿到unitid, original_conversion, kvalue, date, hour
    3. 读取实验配置文件
    4. 关联以上三步得到的数据记录，过滤仅保留第一步中没有ocpc广告记录的、第二部中is_recommend=1的，以及第三部实验配置文件中配置flag=1的unitid
    5. 去重后存储到非时间分区表中
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
//    scp ocpc_abtest.pb cpc@192.168.80.23:/home/cpc/model_server/data/ocpc_abtest.pb
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val conf = ConfigFactory.load("ocpc")
    val expDataPath = conf.getString("ocpc_all.ocpc_exp_flag")
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media")
    println(s"expDataPath=$expDataPath")

    // 抽取前48小时到前24小时是否有ocpc广告记录，生成flag
    val baseData = getOcpcHistoryFlag(media, date, hour, spark)

    // 抽取今天的unitid, original_conversion, kvalue, date, hour
    val kvalue = getSuggestData(version, date, hour, spark)

    // 读取实验配置文件
    val expUnitid = getExpSet(expDataPath, version, date, hour, spark)

    // 数据关联
    val resultDF = assembleData(baseData, kvalue, expUnitid, date, hour, spark)
  }

  def assembleData(baseData: DataFrame, kvalue: DataFrame, expUnitid: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    关联以上三步得到的数据记录，过滤仅保留第一步中没有ocpc广告记录的、第二部中is_recommend=1的，以及第三部实验配置文件中配置flag=1的unitid
     */
    val data = kvalue
      .join(baseData, Seq("identifier", "conversion_goal"), "left_outer")
      .select("identifier", "conversion_goal", "kvalue", "click")
      .na.fill(0, Seq("click"))
      .join(expUnitid, Seq("identifier"), "inner")
      .select("identifier", "conversion_goal", "kvalue", "click", "exp_flag")

    data.show(10)
    val resultDF = data
      .filter(s"click > 0 and exp_flag = 1")
    resultDF.show(10)
    resultDF
  }

  def getExpSet(expDataPath: String, version: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(expDataPath)
    data.show(10)

    val resultDF = data
        .filter(s"version = '$version'")
        .groupBy("identifier")
        .agg(
          min(col("exp_flag")).alias("exp_flag")
        )
        .select("identifier", "exp_flag")

    resultDF.show(10)
    resultDF
  }

  def getSuggestData(version: String, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest1 =
      s"""
         |SELECT
         |  cast(unitid as string) as identifier,
         |  kvalue,
         |  original_conversion as conversion_goal
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |  `date`='$date'
         |AND
         |  version='$version'
         |AND
         |  is_recommend = 1
       """.stripMargin
    println(sqlRequest1)
    val data = spark
      .sql(sqlRequest1)
      .groupBy("identifier", "conversion_goal")
      .agg(avg("kvalue").alias("kvalue"))
      .select("identifier", "kvalue", "conversion_goal")

    data.show(10)
    data
  }

  def getOcpcHistoryFlag(media: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday1 = calendar.getTime
    val date1 = dateConverter.format(yesterday1)
    calendar.add(Calendar.DATE, -1)
    val yesterday2 = calendar.getTime
    val date2 = dateConverter.format(yesterday2)
    val selectCondition = getTimeRangeSql2(date2, hour, date1, hour)

    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    val sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    isclick,
         |    cast(ocpc_log_dict['conversiongoal'] as int) as conversion_goal
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
        .groupBy("unitid", "conversion_goal")
        .agg(sum(col("isclick")).alias("click"))
        .withColumn("identifier", col("unitid"))
        .select("identifier", "conversion_goal", "click")

    resultDF.show(10)
    resultDF
  }


}

