package com.cpc.spark.OcpcProtoType.suggest_cpa_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.cpc.spark.OcpcProtoType.suggest_cpa_v3.OcpcCalculateAUC.OcpcCalculateAUCmain
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcSuggestCPA {
  def main(args: Array[String]): Unit = {
    /*
    新版推荐cpa程序：
    unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, cal_bid, auc, kvalue, industry, is_recommend, ocpc_flag, usertype, pcoc1, pcoc2

    主要源表：dl_cpc.ocpc_base_unionlog, dl_cpc.ocpc_label_cvr_hourly

    数据构成分为以下部分:
    1. 基础数据部分：unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, industry, usertype
    2. ocpc部分：kvalue
    3. 模型部分：auc
    4. 实时查询：ocpc_flag
    5. 历史推荐cpa数据：pcoc1, pcoc2
    6.
     */
    // 计算日期周期
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val hourInt = args(3).toInt


    val spark = SparkSession
      .builder()
      .appName(s"ocpc suggest cpa v2: $date, $hour")
      .enableHiveSupport().getOrCreate()
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version")


    // 按照转化目标抽取基础数据表
    val baseLog = getBaseLog(hourInt, date, hour, spark)

    // 统计数据
    val baseData = calculateLog(baseLog, date, hour, spark)

    // ocpc校准部分
    val kvalue = getKvalue(baseLog, baseData, date, hour, spark)

    // 模型部分
    val aucData = OcpcCalculateAUCmain(date, hour, version, hourInt, spark)

    // 数据组装
    val result = assemblyData(baseData, kvalue, aucData, spark)

    val resultDF = result
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_recommend_units_hourly20190704")
//      .repartition(10).write.mode("overwrite").insertInto("test.ocpc_recommend_units_hourly")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_recommend_hourly_v2")
    println("successfully save data into table: dl_cpc.ocpc_suggest_cpa_recommend_hourly_v2")
  }

  def assemblyData(baseData: DataFrame, kvalue: DataFrame, aucData: DataFrame, spark: SparkSession) = {
    /*
    assemlby the data together
     */
    val rawData = baseData
      .join(kvalue, Seq("unitid", "userid", "conversion_goal", "media", "adclass", "industry", "usertype"), "left_outer")
      .join(aucData, Seq("unitid", "userid", "conversion_goal", "media", "adclass", "industry", "usertype"), "left_outer")
      .select("unitid", "userid", "conversion_goal", "media", "adclass", "industry", "usertype", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pre_cvr", "post_cvr", "pcoc", "cal_bid", "auc")

    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  *,
         |  (case when industry in ('elds', 'feedapp') then 10
         |        else 60 end) as cv_threshold
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    val resultDF = data
      .withColumn("is_recommend", when(col("auc").isNotNull && col("cal_bid").isNotNull && col("cvrcnt").isNotNull, 1).otherwise(0))
      .withColumn("is_recommend", when(col("auc") <= 0.65, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") < 0.7, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") > 1.3, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cvrcnt") < col("cv_threshold"), 0).otherwise(col("is_recommend")))
      .select("unitid", "userid", "conversion_goal", "media", "adclass", "industry", "usertype", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pre_cvr", "post_cvr", "pcoc", "cal_bid", "auc", "is_recommend")

    resultDF

  }

  def getKvalue(baseData: DataFrame, baseStat: DataFrame, date: String, hour: String, spark: SparkSession) = {
    baseStat.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  media,
         |  adclass,
         |  industry,
         |  usertype,
         |  1.0 / pcoc as cali_value,
         |  1.0 / jfb as jfb_factor,
         |  post_cvr,
         |  cpa
         |FROM
         |  base_data
       """.stripMargin
    println(sqlRequest)
    val cvrData = spark.sql(sqlRequest)

    val data = baseData
      .join(cvrData, Seq("unitid", "userid", "conversion_goal", "media", "adclass", "industry", "usertype"), "inner")
      .withColumn("cali_pcvr", col("exp_cvr") * 0.5 * col("cali_value") + col("post_cvr") * 0.5)
      .select("searchid", "unitid", "userid", "conversion_goal", "media", "adclass", "industry", "usertype", "cali_pcvr", "jfb_factor", "cpa")
      .withColumn("cal_bid", col("cali_pcvr") * col("cpa") * col("jfb_factor"))
      .groupBy("unitid", "userid", "conversion_goal", "media", "adclass", "industry", "usertype")
      .agg(
        avg(col("cal_bid")).alias("cal_bid")
      )
      .select("unitid", "userid", "conversion_goal", "media", "adclass", "industry", "usertype", "cal_bid")

    data
  }

  def calculateLog(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // 数据统计: unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, industry, usertype
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  media,
         |  adclass,
         |  industry,
         |  usertype,
         |  sum(isshow) as show,
         |  sum(isclick) as click,
         |  sum(iscvr) as cvrcnt,
         |  sum(case when isclick=1 then price else 0 end) as cost,
         |  sum(isclick) * 1.0 / sum(isshow) as post_ctr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as acb,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(case when isclick=1 then bid else 0 end) as jfb,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpa,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(iscvr) * 1.0 / sum(isclick) as post_cvr
         |FROM
         |  raw_data
         |GROUP BY unitid, userid, conversion_goal, media, adclass, industry, usertype
       """.stripMargin
    println(sqlRequest1)
    val data = spark
      .sql(sqlRequest1)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))

    // 数据关联
    val resultDF = data
      .select("unitid", "userid", "conversion_goal", "media", "adclass", "industry", "usertype", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pre_cvr", "post_cvr", "pcoc")

    resultDF
  }

  def getBaseLog(hourCnt: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取基础数据用于后续计算与统计
    unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, industry, usertype
     */
    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 时间区间选择
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val endDay = date + " " + hour
    val endDayTime = dateConverter.parse(endDay)
    val calendar = Calendar.getInstance
    calendar.setTime(endDayTime)
    calendar.add(Calendar.HOUR, -hourCnt)
    val startDateTime = calendar.getTime
    val startDateStr = dateConverter.format(startDateTime)
    val date1 = startDateStr.split(" ")(0)
    val hour1 = startDateStr.split(" ")(1)
    val timeSelection = getTimeRangeSqlDate(date1, hour1, date, hour)

    // 抽取点击数据: dl_cpc.ocpc_base_unionlog
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    adclass,
         |    isshow,
         |    isclick,
         |    price,
         |    bid_discounted_by_ad_slot as bid,
         |    (case
         |        when media_appsid in ('80000001', '80000002') then 'qtt'
         |        when media_appsid in ('80002819') then 'hottopic'
         |        else 'novel'
         |    end) as media,
         |    (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |    end) as industry,
         |    usertype,
         |    exp_cvr,
         |    exp_ctr,
         |    conversion_goal
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    $timeSelection
         |AND
         |    $mediaSelection
         |AND
         |    antispam = 0
         |AND
         |    adslot_type in (1,2,3)
         |AND
         |    adsrc = 1
         |AND
         |    (charge_type is null or charge_type = 1)
         |AND
         |    conversion_goal > 0
         |AND
         |    is_ocpc = 1
       """.stripMargin
    println(sqlRequest1)
    val ctrData = spark
      .sql(sqlRequest1)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))

    // 抽取转化数据
    val sqlRequest2 =
      s"""
         |SELECT
         |    searchid,
         |    label as iscvr,
         |    cvr_goal
         |FROM
         |    dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |    `date` >= '$date1'
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2)

    // 数据关联
    val data = ctrData
      .join(cvrData, Seq("searchid", "cvr_goal"), "left_outer")

    data.show(10)
    data
  }

}
