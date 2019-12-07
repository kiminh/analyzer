package com.cpc.spark.oCPX.oCPC.report

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{udfConcatStringInt, udfDetermineIndustry, udfDetermineMedia}
import com.typesafe.config.ConfigFactory

import com.cpc.spark.ocpcV3.utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object OcpcHourlyGeneralData {
  def main(args: Array[String]): Unit = {
    /*
    1. 从base表抽取数据，按照行业过滤数据
    2. 统计各项指标
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version")

    val ocpcRawData = getOcpcData(date, hour, spark)
    val cpcRawData = getCpcData(date, hour, spark)

    // 统计汇总数据
    val cpcData = getCPCstats(cpcRawData, date, hour, spark)
    val ocpcData = getOCPCstats(ocpcRawData, date, hour, spark)
    //auc
    val aucData = getAuc(ocpcRawData, date, hour, spark)

    val joinData = ocpcData
      .join(cpcData, Seq("industry", "conversion_goal", "media", "ocpc_expand"), "inner")
      .join(aucData, Seq("industry", "conversion_goal", "media", "ocpc_expand"), "inner")

    // 计算前一天数据
    val result1 = joinData
      .withColumn("cost_cmp", lit(0.1))
      .withColumn("cost_ratio", col("ocpc_cost") * 1.0 / col("cost"))
      .withColumn("cost_low", col("low_cost") * 0.01)
      .withColumn("cost_high", col("high_cost") * 0.01)
      .withColumn("low_unit_percent", col("low_unitid_cnt") * 1.0 / col("unitid_cnt"))
      .withColumn("pay_percent", col("high_cost") * 1.0 / col("ocpc_cost"))
      //.withColumn("auc", col("high_cost") )

    val prevData = getPrevData(date, hour, version, spark)
    val result2 = result1
        .join(prevData, Seq("industry", "conversion_goal", "media", "ocpc_expand"), "left_outer")
        .na.fill(0.0, Seq("cost_yesterday"))
        .withColumn("cost_cmp", when(col("cost_yesterday") === 0.0, 1.0).otherwise((col("ocpc_cost") * 0.01 - col("cost_yesterday")) / col("cost_yesterday")))
    val result = result2

    result.show(10)

    val resultDF = result
      .withColumn("cost", col("ocpc_cost") * 0.01)
      .select("industry", "cost", "cost_cmp", "cost_ratio", "cost_low", "cost_high", "unitid_cnt", "userid_cnt", "low_unit_percent", "pay_percent", "cpa_real", "cpa_given", "conversion_goal", "media", "ocpc_expand", "pre_cvr", "post_cvr", "auc")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .cache()

    resultDF.show(10)

    resultDF
      .select("industry", "cost", "cost_cmp", "cost_ratio", "cost_low", "cost_high", "unitid_cnt", "userid_cnt", "low_unit_percent", "pay_percent", "conversion_goal", "media",  "ocpc_expand", "pre_cvr", "post_cvr", "auc", "date", "hour", "version")
      .repartition(1).write.mode("overwrite").insertInto("test.ocpc_general_data_industry_hourly")
//      .repartition(1).write.mode("overwrite").insertInto("dl_cpc.ocpc_general_data_industry_hourly")


  }

  def getPrevData(date: String, hour: String, version: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` = '$date1' and `hour` = '$hour' and version = '$version'"

    val sqlRequest =
      s"""
         |SELECT
         |  industry,
         |  conversion_goal,
         |  media,
         |  ocpc_expand,
         |  cost as cost_yesterday
         |FROM
         |  test.ocpc_general_data_industry_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getOCPCstats(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  industry,
         |  conversion_goal,
         |  media,
         |  ocpc_expand,
         |  unitid,
         |  userid,
         |  sum(case when isclick=1 then price else 0 end) as ocpc_cost,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpagiven
         |FROM
         |  raw_data
         |GROUP BY industry, conversion_goal, media, ocpc_expand, unitid, userid
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val baseData = data
      .withColumn("pred_cost", col("cv") * col("cpagiven"))
      .withColumn("high_cost", col("ocpc_cost") -  col("pred_cost") * 1.2)
      .withColumn("high_cost", when(col("high_cost") <= 0, 0.0).otherwise(col("high_cost")))

//    baseData.write.mode("overwrite").saveAsTable("test.ocpc_general_data_industry20191016a")

    baseData.createOrReplaceTempView("base_data")
    val sqlRequest2 =
      s"""
         |SELECT
         |  industry,
         |  conversion_goal,
         |  media,
         |  ocpc_expand,
         |  sum(ocpc_cost) as ocpc_cost,
         |  sum(high_cost) as high_cost,
         |  sum(case when high_cost = 0.0 then 1 else 0 end) as low_unitid_cnt,
         |  count(unitid) as unitid_cnt,
         |  count(distinct userid) as userid_cnt
         |FROM
         |  base_data
         |GROUP BY industry, conversion_goal, media, ocpc_expand
       """.stripMargin
    println(sqlRequest2)
    val result1 = spark
      .sql(sqlRequest2)
      .withColumn("low_cost", col("ocpc_cost") - col("high_cost"))

    val sqlRequest3 =
      s"""
         |SELECT
         |  industry,
         |  conversion_goal,
         |  media,
         |  ocpc_expand,
         |  sum(case when isclick=1 then cpagiven else 0 end) * 1.0 / sum(isclick) as cpa_given,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpa_real,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(iscvr) * 1.0  / sum(isclick) as post_cvr
         |FROM
         |  raw_data
         |GROUP BY industry, conversion_goal, media, ocpc_expand
       """.stripMargin
    println(sqlRequest3)
    val result2 = spark.sql(sqlRequest3)

    val result = result1
      .join(result2, Seq("industry", "conversion_goal", "media", "ocpc_expand"), "inner")

    result

  }

  def getCPCstats(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  industry,
         |  conversion_goal,
         |  media,
         |  ocpc_expand,
         |  sum(case when isclick=1 then price else 0 end) as cost
         |FROM
         |  raw_data
         |GROUP BY industry, conversion_goal, media,ocpc_expand
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getCpcData(date: String, hour: String, spark: SparkSession) = {
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
         |    isclick,
         |    isshow,
         |    price,
         |    media_appsid,
         |    ocpc_expand
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    `date` = '$date'
         |and `hour` <= '$hour'
         |and isshow = 1
         |and conversion_goal > 0
       """.stripMargin
    println(sqlRequest1)
    val result = spark
      .sql(sqlRequest1)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("industry", udfDetermineIndustry()(col("adslot_type"), col("adclass")))

    result
  }

  def getOcpcData(date: String, hour: String, spark: SparkSession) = {
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
         |    isclick,
         |    isshow,
         |    price,
         |    bid_discounted_by_ad_slot as bid,
         |    exp_cvr,
         |    exp_ctr,
         |    media_appsid,
         |    cast(ocpc_log_dict['cpagiven'] as double) as cpagiven,
         |    ocpc_expand
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

  def getAuc(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_data")
    import spark.implicits._
    val sqlRequest =
      s"""
         |SELECT
         |  industry,
         |  conversion_goal,
         |  media,
         |  ocpc_expand,
         |  case when isclick=1 then exp_cvr else 0 end as score,
         |  iscvr as label
         |FROM
         |  raw_data
      """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)


    val newData = data
      .withColumn("id", concat_ws("-", col("industry"), col("conversion_goal"), col("media"), col("ocpc_expand")))
      .selectExpr("id", "cast(score as int) score", "label")
      .coalesce(400)

    newData.show(10)

    val result = utils.getGauc(spark, newData, "id")
    val resultRDD = result.rdd.map(row => {
      val id = row.getAs[String]("name")
      val identifierList = id.trim.split("-")
      val industry = identifierList(0)
      val conversionGoal = identifierList(1).toInt
      val media = identifierList(2)
      val ocpc_expand = identifierList(3)
      val auc = row.getAs[Double]("auc")
      (industry, conversionGoal, media, ocpc_expand, auc)
    })
    val resultDF = resultRDD.toDF("industry", "conversion_goal", "media", "ocpc_expand", "auc")
    resultDF
  }

}
