package com.cpc.spark.OcpcProtoType.model_v6

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcRangeCalibrationV2 {
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
    val media = args(3).toString
    val hourInt = args(4).toInt
    val minCV = args(5).toInt
    val expTag = args(6).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, version:$version, hourInt:$hourInt, minCV:$minCV, expTag:$expTag")

    // 抽取基础数据
    val result = OcpcRangeCalibrationMain(date, hour, version, media, hourInt, minCV, expTag, spark)

    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_ocpc_range_calibration20190702a")

  }

  def OcpcRangeCalibrationMain(date: String, hour: String, version: String, media: String, hourInt: Int, minCV: Int, expTag: String, spark:SparkSession) = {
    /*
    计算新版的cvr平滑策略：
    1. 抽取基础数据
    2. 计算该维度下pcoc与计费比、后验cvr等等指标
    3. 计算该维度下根据给定highBidFactor计算出的lowBidFactor
     */

    // 抽取基础数据
    val baseDataClick = getBaseData(media, hourInt, date, hour, spark)
    val cvrData = getCvrData(hourInt, date, hour, spark)
    val baseData = baseDataClick
      .join(cvrData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))
      .select("searchid", "unitid", "bid", "price", "exp_cvr", "isclick", "isshow", "iscvr", "conversion_goal")

    // 计算各维度下的pcoc、jfb以及后验cvr等指标
    val dataRaw1 = calculateData1(baseData, version, expTag, date, hour, spark)
    val data1 = dataRaw1
        .filter(s"cv >= $minCV")
        .cache()
    data1.show(10)



    // 计算该维度下根据给定highBidFactor计算出的lowBidFactor
    val baseData2 = baseData
      .join(data1, Seq("unitid", "conversion_goal"), "inner")

    val data2 = calculateData2(baseData2, date, hour, spark)

    val resultDF = data1
      .select("unitid", "conversion_goal", "post_cvr", "pcoc", "jfb")
      .join(data2, Seq("unitid", "conversion_goal"), "inner")
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor")

    resultDF

  }

  def getBidFactor(version: String, expTag: String, spark: SparkSession) = {
    // 从配置文件读取数据
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_config.bid_factor")
    val rawData = spark.read.format("json").json(confPath)
    val data = rawData
      .filter(s"version = '$version' and exp_tag = '$expTag'")
      .select("conversion_goal", "high_bid_factor", "low_bid_factor")
      .distinct()

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
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then pcvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  avg(high_bid_factor) as high_bid_factor,
         |  avg(low_bid_factor) as low_bid_factor
         |FROM
         |  raw_data
         |GROUP BY unitid, conversion_goal
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("calc_total", col("pre_cvr") * col("click"))
      .select("unitid", "conversion_goal", "calc_total", "high_bid_factor", "low_bid_factor")

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then pcvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  avg(high_bid_factor) as high_bid_factor,
         |  avg(low_bid_factor) as low_bid_factor
         |FROM
         |  raw_data
         |WHERE
         |  pcvr_group = "high"
         |GROUP BY unitid, conversion_goal
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .withColumn("calc_high", col("pre_cvr") * col("click") * col("high_bid_factor"))
      .select("unitid", "conversion_goal", "calc_high")

    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then pcvr else 0 end) * 1.0 / sum(isclick) as pre_cvr
         |FROM
         |  raw_data
         |WHERE
         |  pcvr_group = "low"
         |GROUP BY unitid, conversion_goal
       """.stripMargin
    println(sqlRequest3)
    val data3 = spark
      .sql(sqlRequest3)
      .withColumn("calc_low", col("pre_cvr") * col("click"))
      .select("unitid", "conversion_goal", "calc_low")

    val data = data1
      .join(data2, Seq("unitid", "conversion_goal"), "inner")
      .join(data3, Seq("unitid", "conversion_goal"), "inner")
      .select("unitid", "conversion_goal", "calc_total", "calc_high", "calc_low", "high_bid_factor", "low_bid_factor")
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
         |  sum(iscvr) * 1.0 / sum(isclick) as post_cvr,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as acb,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv
         |FROM
         |  base_data
         |GROUP BY unitid, conversion_goal
       """.stripMargin
    println(sqlRequest)
    val data1 = spark
      .sql(sqlRequest)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .select("unitid", "conversion_goal", "post_cvr", "pre_cvr", "acp", "acb", "pcoc", "jfb", "click", "cv")

    // 抽取highbidfactor与lowbidfactor
    val data2 = getBidFactor(version, expTag, spark)

    // 数据关联
    val data = data1
      .join(data2, Seq("conversion_goal"), "left_outer")
      .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor"))

    data
  }

  def getCvrData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getBaseData(media: String, hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    val expTag: Nothing = 1
    val unitid: Nothing = 2
    val ideaid: Nothing = 3
    val slotid: Nothing = 4
    val slottype: Nothing = 5
    val adtype: Nothing = 6
     */
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  unitid,
         |  adslotid as slotid,
         |  adslot_type as slottype,
         |  adtype,
         |  bid_discounted_by_ad_slot as bid,
         |  price,
         |  exp_cvr,
         |  isclick,
         |  isshow,
         |  conversion_goal
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  price <= bid_discounted_by_ad_slot
         |AND
         |  is_ocpc = 1
         |AND
         |  conversion_goal > 0
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))

    data
  }


}
