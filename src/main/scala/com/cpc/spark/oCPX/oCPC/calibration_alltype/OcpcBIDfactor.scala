package com.cpc.spark.oCPX.oCPC.calibration_alltype

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.oCPC.calibration_alltype.udfs._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcBIDfactor {
  def main(args: Array[String]): Unit = {
    /*
    val expTag: Nothing = 1
    val unitid: Nothing = 2
    val ideaid: Nothing = 3
    val slotid: Nothing = 4
    val slottype: Nothing = 5
    val adtype: Nothing = 6
    val cvrCalFactor: Double = 7
    val jfbFactor: Double = 8
    val postCvr: Double = 9
    val highBidFactor: Double = 10
    val lowBidFactor: Double = 11
    计算新版的cvr平滑策略：
    1. 抽取基础数据
    2. 计算该维度下pcoc与计费比、后验cvr等等指标
    3. 计算该维度下根据给定highBidFactor计算出的lowBidFactor
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = args(4).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, expTag:$expTag, version:$version, hourInt:$hourInt")

    // 抽取基础数据
    val result = OcpcBIDfactorMain(date, hour, version, expTag, hourInt, spark)

    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_bid_factor20190723a")

  }

  def OcpcBIDfactorMain(date: String, hour: String, version: String, expTag: String, hourInt: Int, spark:SparkSession) = {
    /*
    计算新版的cvr平滑策略：
    1. 抽取基础数据
    2. 计算该维度下pcoc与计费比、后验cvr等等指标
    3. 计算该维度下根据给定highBidFactor计算出的lowBidFactor
     */

    // 抽取基础数据
    val baseDataRaw = getBaseData(hourInt, date, hour, spark)
    val baseData = baseDataRaw
      .withColumn("adslot_type", udfAdslotTypeMapAs()(col("adslot_type")))
      .withColumn("identifier", udfGenerateId()(col("unitid"), col("adslot_type")))


    // 计算各维度下的pcoc、jfb以及后验cvr等指标
    val dataRaw1 = calculateData1(baseData, version, expTag, date, hour, spark)
//    dataRaw1
//      .repartition(10).write.mode("overwrite").saveAsTable("test.check_bid_factor20190723c")
    val data1 = dataRaw1
        .filter(s"cv >= min_cv")
        .cache()
    data1.show(10)
//    data1
//      .repartition(10).write.mode("overwrite").saveAsTable("test.check_bid_factor20190723d")



    // 计算该维度下根据给定highBidFactor计算出的lowBidFactor
    val baseData2 = baseData
      .withColumn("media", udfMediaName()(col("media")))
      .join(data1, Seq("unitid", "conversion_goal", "media"), "inner")

    val data2 = calculateData2(baseData2, date, hour, spark)

    val resultDF = data1
      .select("unitid", "conversion_goal", "media", "exp_tag", "cv")
      .join(data2, Seq("unitid", "conversion_goal", "media"), "inner")
      .selectExpr("unitid", "conversion_goal", "exp_tag", "high_bid_factor", "low_bid_factor")
      .withColumn("version", lit(version))

    resultDF

  }

  def getBidFactor(version: String, expTag: String, spark: SparkSession) = {
    // 从配置文件读取数据
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_config.bid_factor_v2")
    val rawData = spark.read.format("json").json(confPath)
    val data = rawData
      .filter(s"version = '$version'")
      .select("exp_tag", "conversion_goal", "high_bid_factor", "low_bid_factor", "min_cv")
      .distinct()

    println("bid factor config:")
    data.show(10)

    data

  }

  def calculateData2(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    val expTag: Nothing = 1
    val unitid: Nothing = 2
    val ideaid: Nothing = 3
    val slotid: Nothing = 4
    val slottype: Nothing = 5
    val adtype: Nothing = 6
     */
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  conversion_goal,
         |  media,
         |  bid,
         |  price,
         |  exp_cvr,
         |  isclick,
         |  isshow,
         |  exp_cvr * 1.0 / pcoc as pcvr,
         |  post_cvr,
         |  high_bid_factor,
         |  low_bid_factor
         |FROM
         |  base_data
         |WHERE
         |  conversion_goal > 0
         |AND
         |  conversion_goal is not null
       """.stripMargin
    println(sqlRequest)
    val rawData = spark
      .sql(sqlRequest)
      .withColumn("pcvr_group", when(col("pcvr") >= col("post_cvr"), "high").otherwise("low"))


    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  media,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then pcvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  avg(high_bid_factor) as high_bid_factor,
         |  avg(low_bid_factor) as low_bid_factor
         |FROM
         |  raw_data
         |GROUP BY unitid, conversion_goal, media
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("calc_total", col("pre_cvr") * col("click"))
      .select("unitid", "conversion_goal", "media", "calc_total", "high_bid_factor", "low_bid_factor")

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  media,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then pcvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  avg(high_bid_factor) as high_bid_factor,
         |  avg(low_bid_factor) as low_bid_factor
         |FROM
         |  raw_data
         |WHERE
         |  pcvr_group = "high"
         |GROUP BY unitid, conversion_goal, media
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .withColumn("calc_high", col("pre_cvr") * col("click") * col("high_bid_factor"))
      .select("unitid", "conversion_goal", "media", "calc_high")

    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  media,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then pcvr else 0 end) * 1.0 / sum(isclick) as pre_cvr
         |FROM
         |  raw_data
         |WHERE
         |  pcvr_group = "low"
         |GROUP BY unitid, conversion_goal, media
       """.stripMargin
    println(sqlRequest3)
    val data3 = spark
      .sql(sqlRequest3)
      .withColumn("calc_low", col("pre_cvr") * col("click"))
      .select("unitid", "conversion_goal", "media", "calc_low")

    val data = data1
      .join(data2, Seq("unitid", "conversion_goal", "media"), "inner")
      .join(data3, Seq("unitid", "conversion_goal", "media"), "inner")
      .select("unitid", "conversion_goal", "media", "calc_total", "calc_high", "calc_low", "high_bid_factor", "low_bid_factor")
      .withColumn("low_bid_factor", udfCalculateLowBidFactor()(col("calc_total"), col("calc_high"), col("calc_low"), col("low_bid_factor")))

    data
  }

  def udfCalculateLowBidFactor() = udf((calcTotal: Double, calcHigh: Double, calcLow: Double, lowBidFactor: Double) => {
    var result = 1.0 * (calcTotal - calcHigh) / calcLow
    if (result < lowBidFactor) {
      result = lowBidFactor
    }
    result
  })

  def calculateData1(baseData: DataFrame, version: String, expTag: String, date: String, hour: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  media,
         |  sum(iscvr) * 1.0 / sum(isclick) as post_cvr,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as acb,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv
         |FROM
         |  base_data
         |GROUP BY unitid, conversion_goal, media
       """.stripMargin
    println(sqlRequest)
    val data1 = spark
      .sql(sqlRequest)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .select("unitid", "conversion_goal", "media", "post_cvr", "pre_cvr", "click", "cv", "pcoc", "jfb")
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .select("unitid", "conversion_goal", "media", "exp_tag", "post_cvr", "pre_cvr", "click", "cv", "pcoc", "jfb")

    // 抽取highbidfactor与lowbidfactor, min_cv
    // 获取相关参数
    // highbidfactor, lowbidfactor: 默认为1
    // min_cv: 默认为40
    val data2 = getBidFactor(version, expTag, spark)

    // 数据关联
    val data = data1
      .join(data2, Seq("exp_tag", "conversion_goal"), "left_outer")
      .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor"))
      .na.fill(40, Seq("min_cv"))


    data
  }


}
