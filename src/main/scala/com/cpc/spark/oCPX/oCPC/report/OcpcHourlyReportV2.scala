package com.cpc.spark.oCPX.oCPC.report

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcHourlyReportV2 {
  def main(args: Array[String]): Unit = {
    /*
    新版报表程序
    1. 从ocpc_unionlog拉取ocpc广告记录
    2. 采用数据关联方式获取转化数据
    3. 统计分ideaid级别相关数据
    4. 统计分conversion_goal级别相关数据
    5. 存储到hdfs
    6. 存储到mysql
     */
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    println("parameters:")
    println(s"date=$date, hour=$hour")

    // 拉取点击、消费、转化等基础数据
    val rawData = getBaseData(date, hour, spark)

  }

  def getBaseData(date: String, hour: String, spark: SparkSession) = {
    /**
      * 重新计算抽取全天截止当前时间的数据日志
      */
    // 抽取基础数据：所有跑ocpc的广告主
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    userid,
         |    adslot_type,
         |    adclass,
         |    conversion_goal,
         |    conversion_from,
         |    deep_conversion_goal,
         |    cpa_check_priority,
         |    is_deep_ocpc,
         |    ocpc_expand,
         |    isclick,
         |    isshow,
         |    price,
         |    bid_discounted_by_ad_slot as bid,
         |    exp_cvr,
         |    raw_cvr * 1.0 / 1000000 as raw_cvr,
         |    exp_ctr,
         |    media_appsid,
         |    cast(exp_cpm as double) / 1000000 as exp_cpm,
         |    hour as hr,
         |    hidden_tax,
         |    ocpc_step,
         |    deep_ocpc_step,
         |    ocpc_log,
         |    deep_ocpc_log
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    `date` = '$date'
         |and `hour` <= '$hour'
         |and isshow = 1
         |and conversion_goal > 0
       """.stripMargin
    println(sqlRequest1)
    val clickDataRaw = spark.sql(sqlRequest1)

    val clickData = mapMediaName(clickDataRaw, spark)

    // 关联浅层转化表
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  conversion_goal,
         |  conversion_from,
         |  1 as iscvr1
         |FROM
         |  dl_cpc.ocpc_cvr_log_hourly
         |WHERE
         |  date >= '$date'
       """.stripMargin
    println(sqlRequest2)
    val cvData1 = spark.sql(sqlRequest2).distinct()

    // 关联深层转化表
    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  deep_conversion_goal,
         |  1 as iscvr2
         |FROM
         |  dl_cpc.ocpc_label_deep_cvr_hourly
         |WHERE
         |  date >= '$date'
       """.stripMargin
    println(sqlRequest3)
    val cvData2 = spark.sql(sqlRequest3).distinct()


    // 数据关联
    val resultDF = clickData
      .join(cvData1, Seq("searchid", "conversion_goal", "conversion_from"), "left_outer")
      .join(cvData2, Seq("searchid", "deep_conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr1", "iscvr2"))

    resultDF

  }


}