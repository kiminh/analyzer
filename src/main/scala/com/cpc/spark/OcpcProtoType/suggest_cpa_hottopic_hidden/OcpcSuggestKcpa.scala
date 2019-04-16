package com.cpc.spark.OcpcProtoType.suggest_cpa_hottopic_hidden

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.suggest_cpa_v1.OcpcSuggestKcpa._

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
    resultDF.repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_k")
    resultDF.repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_k_version")


  }



}

