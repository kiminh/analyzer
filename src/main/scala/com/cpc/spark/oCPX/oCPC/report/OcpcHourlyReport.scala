package com.cpc.spark.oCPX.oCPC.report

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.tools.testOperateMySQL
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcHourlyReport {
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

    // 分ideaid和conversion_goal统计数据
    val baseData = calculateBaseData(rawData, spark)

    // 深度转化数据
    val deepRawData = getDeepBaseData(date, hour, spark)

    // 统计深度转化指标
    val deepBaseData = calculateDeepBaseData(deepRawData, spark)

    // 数据关联
    val data = baseData
        .join(deepBaseData, Seq("ideaid", "unitid", "userid", "adclass", "adslot_type", "conversion_goal", "deep_conversion_goal", "cpa_check_priority", "is_deep_ocpc", "industry", "media", "hr", "ocpc_expand", "is_hidden"), "left_outer")

    // 存储数据到hadoop
    saveBaseDataToHDFS(data, date, hour, spark)


  }

  def saveBaseDataToHDFS(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val resultDF = data
      .withColumn("date", lit(date))
      .withColumn("hour", col("hr"))
      .select("ideaid", "unitid", "userid", "adclass", "conversion_goal", "industry", "media", "show", "click", "cv", "total_price", "total_bid", "total_precvr", "total_prectr", "total_cpagiven", "total_jfbfactor", "total_cvrfactor", "total_calipcvr", "total_calipostcvr", "total_cpasuggest", "total_smooth_factor", "is_hidden", "adslot_type", "total_exp_cpm", "total_rawcvr", "deep_conversion_goal", "cpa_check_priority", "is_deep_ocpc", "deep_click", "deep_cv", "total_deepcvr", "total_deep_cpagiven", "total_deep_jfbfactor", "total_deep_cvrfactor", "total_deep_calipcvr", "total_deep_smooth_factor", "real_deep_click", "total_deep_price", "total_deep_bid", "ocpc_expand", "date", "hour")
      .filter(s"date is not null and hour is not null")
      .na.fill(0, Seq("impression", "click", "cv", "total_price", "total_bid", "total_precvr", "total_prectr", "total_cpagiven", "total_jfbfactor", "total_cvrfactor", "total_calipcvr", "total_calipostcvr", "total_cpasuggest", "total_smooth_factor", "is_hidden", "adslot_type", "total_exp_cpm", "total_rawcvr", "deep_conversion_goal", "cpa_check_priority", "is_deep_ocpc", "deep_click", "deep_cv", "total_deepcvr", "total_deep_cpagiven", "total_deep_jfbfactor", "total_deep_cvrfactor", "total_deep_calipcvr", "total_deep_smooth_factor", "real_deep_click", "total_deep_price", "total_deep_bid", "ocpc_expand"))


    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_report_base_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_report_base_hourly")
  }

  def calculateBaseData(rawData: DataFrame, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  unitid,
         |  userid,
         |  adclass,
         |  adslot_type,
         |  conversion_goal,
         |  deep_conversion_goal,
         |  cpa_check_priority,
         |  is_deep_ocpc,
         |  industry,
         |  media,
         |  hr,
         |  ocpc_expand,
         |  0 as is_hidden,
         |  sum(isshow) as show,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then price else 0 end) as total_price,
         |  sum(case when isclick=1 then bid else 0 end) as total_bid,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 as total_precvr,
         |  sum(case when isclick=1 then raw_cvr else 0 end) * 1.0 as total_rawcvr,
         |  sum(case when isshow=1 then exp_ctr else 0 end) * 1.0 as total_prectr,
         |  sum(case when isshow=1 then exp_cpm else 0 end) * 1.0 as total_exp_cpm,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['cpagiven'] as double) else 0 end) as total_cpagiven,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['kvalue'] as double) else 0 end) * 1.0 as total_jfbfactor,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['cvrCalFactor'] as double) else 0 end) * 1.0 as total_cvrfactor,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['pcvr'] as double) else 0 end) * 1.0 as total_calipcvr,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['postCvr'] as double) else 0 end) * 1.0 as total_calipostcvr,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['CpaSuggest'] as double) else 0 end) as total_cpasuggest,
         |  sum(case when isclick=1 then cast(ocpc_log_dict['smoothFactor'] as double) else 0 end) * 1.0 as total_smooth_factor
         |FROM
         |  raw_data
         |GROUP BY ideaid, unitid, userid, adclass, adslot_type, conversion_goal, deep_conversion_goal, cpa_check_priority, is_deep_ocpc, industry, media, hr, ocpc_expand
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    println("base_data:")
    data.show(10)
    data
  }

  def getBaseData(date: String, hour: String, spark: SparkSession) = {
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
         |    ideaid,
         |    unitid,
         |    userid,
         |    adslot_type,
         |    adclass,
         |    conversion_goal,
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
         |    ocpc_log_dict,
         |    cast(exp_cpm as double) / 1000000 as exp_cpm,
         |    hour as hr
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    `date` = '$date'
         |and `hour` <= '$hour'
         |and isshow = 1
         |and conversion_goal > 0
       """.stripMargin
    println(sqlRequest1)
    val clickData = spark
      .sql(sqlRequest1)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("industry", udfDetermineIndustry()(col("adslot_type"), col("adclass")))


    // 关联转化表
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  date >= '$date'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()

    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF

  }


  def getDeepBaseData(date: String, hour: String, spark: SparkSession) = {
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
         |    ideaid,
         |    unitid,
         |    userid,
         |    adslot_type,
         |    adclass,
         |    conversion_goal,
         |    deep_conversion_goal,
         |    cpa_check_priority,
         |    is_deep_ocpc,
         |    ocpc_expand,
         |    isclick,
         |    isshow,
         |    price,
         |    bid_discounted_by_ad_slot as bid,
         |    deep_cvr * 1.0 / 1000000 as deep_cvr,
         |    deep_cpa,
         |    media_appsid,
         |    ocpc_log_dict,
         |    deep_ocpc_log_dict,
         |    (case when deep_ocpc_log_dict is not null then 1 else 0 end) as real_deep_flag,
         |    hour as hr
         |FROM
         |    dl_cpc.ocpc_filter_unionlog
         |WHERE
         |    `date` = '$date'
         |and `hour` <= '$hour'
         |and isshow = 1
         |and conversion_goal > 0
         |and is_deep_ocpc=1
       """.stripMargin
    println(sqlRequest1)
    val clickData = spark
      .sql(sqlRequest1)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("industry", udfDetermineIndustry()(col("adslot_type"), col("adclass")))


    // 关联转化表
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  deep_conversion_goal
         |FROM
         |  dl_cpc.ocpc_label_deep_cvr_hourly
         |WHERE
         |  date >= '$date'
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()

    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "deep_conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF

  }

  def calculateDeepBaseData(rawData: DataFrame, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  unitid,
         |  userid,
         |  adclass,
         |  adslot_type,
         |  conversion_goal,
         |  deep_conversion_goal,
         |  cpa_check_priority,
         |  is_deep_ocpc,
         |  industry,
         |  media,
         |  hr,
         |  ocpc_expand,
         |  0 as is_hidden,
         |  sum(isclick) as deep_click,
         |  sum(iscvr) as deep_cv,
         |  sum(case when isclick=1 then price else 0 end) as total_deep_price,
         |  sum(case when isclick=1 then bid else 0 end) as total_deep_bid,
         |  sum(case when isclick=1 then deep_cvr else 0 end) * 1.0 as total_deepcvr,
         |  sum(case when isclick=1 then cast(deep_cpa as double) else 0 end) as total_deep_cpagiven,
         |  sum(case when isclick=1 and real_deep_flag = 1 then cast(deep_ocpc_log_dict['kvalue'] as double) else 0 end) * 1.0 as total_deep_jfbfactor,
         |  sum(case when isclick=1 and real_deep_flag = 1 then cast(deep_ocpc_log_dict['cvrCalFactor'] as double) else 0 end) * 1.0 as total_deep_cvrfactor,
         |  sum(case when isclick=1 and real_deep_flag = 1 then cast(deep_ocpc_log_dict['pcvr'] as double) else 0 end) * 1.0 as total_deep_calipcvr,
         |  sum(case when isclick=1 and real_deep_flag = 1 then cast(deep_ocpc_log_dict['smoothFactor'] as double) else 0 end) * 1.0 as total_deep_smooth_factor,
         |  sum(case when isclick=1 and real_deep_flag = 1 then 1 else 0 end) as real_deep_click
         |FROM
         |  raw_data
         |GROUP BY ideaid, unitid, userid, adclass, adslot_type, conversion_goal, deep_conversion_goal, cpa_check_priority, is_deep_ocpc, industry, media, hr, ocpc_expand
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    println("base_deep_data:")
    data.show(10)
    data
  }

}