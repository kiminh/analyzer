package com.cpc.spark.oCPX.oCPC.report

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.tools.testOperateMySQL
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcDailyCostReport {
  def main(args: Array[String]): Unit = {
    /*
    分天统计当天的所有单元的消费数据
    只统计到3月底
     */
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    println("parameters:")
    println(s"date=$date")

    // 拉取点击、消费、转化等基础数据
    val baseData = getBaseData(date, spark)

    baseData
      .withColumn("date", lit(date))
      .repartition(5)
      .write.mode("overwrite").saveAsTable("test.ocpc_total_cost_daily")


  }

  def getBaseData(date: String, spark: SparkSession) = {
    /**
      * 重新计算抽取全天截止当前时间的数据日志
      */
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 抽取基础数据：所有跑ocpc的广告主
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    adslot_type,
         |    adclass,
         |    isclick,
         |    isshow,
         |    price,
         |    media_appsid,
         |    ocpc_log,
         |    (case when len(ocpc_log) > 0 then 2 else 1 end) as ocpc_step,
         |    (case when ocpc_log like '%IsHiddenOcpc:1%' as 1 else 0 end) as is_hidden
         |FROM
         |    dl_cpc.cpc_union_log
         |WHERE
         |    `date` = '$date'
         |and $mediaSelection
         |and is_ocpc=1
       """.stripMargin
    println(sqlRequest1)
    val rawData = spark
      .sql(sqlRequest1)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("industry", udfDetermineIndustry()(col("adslot_type"), col("adclass")))
      .filter(s"ocpc_step=1 or (ocpc_step=2 and is_hidden=0)")

    rawData.createOrReplaceTempView("raw_data")

    // 数据关联
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  adslot_type,
         |  media,
         |  adclass,
         |  ocpc_step,
         |  sum(isshow) as show,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then price else 0 end) * 0.01 as cost
         |FROM
         |  raw_data
         |GROUP BY unitid, userid, adslot_type, media, adclass, ocpc_step
       """.stripMargin
    println(sqlRequest2)
    val resultDF = spark.sql(sqlRequest2)

    resultDF

  }

}