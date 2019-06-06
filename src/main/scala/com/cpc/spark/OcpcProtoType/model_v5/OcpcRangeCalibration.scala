package com.cpc.spark.OcpcProtoType.model_v5

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcRangeCalibration {
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
    val highBidFactor = args(4).toDouble
    val lowBidFactor = args(5).toDouble
    val hourInt = args(6).toInt
    val conversionGoal = args(7).toInt
    val minCV = args(8).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, version:$version, highBidFactor:$highBidFactor, lowBidFactor:$lowBidFactor, hourInt:$hourInt, conversionGoal:$conversionGoal, minCV:$minCV")

    // 抽取基础数据
    OcpcRangeCalibration(date, hour, version, media, highBidFactor, lowBidFactor, hourInt, conversionGoal, minCV, spark)

//    result.show(10)

  }

  def OcpcRangeCalibration(date: String, hour: String, version: String, media: String, highBidFactor: Double, lowBidFactor: Double, hourInt: Int, conversionGoal: Int, minCV: Int, spark:SparkSession) = {
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

    // 抽取基础数据
    val cvrType = "cvr" + conversionGoal.toString
    val baseDataClick = getBaseData(media, hourInt, conversionGoal, date, hour, spark)
    val cvrData = getCvrData(cvrType, hourInt, date, hour, spark)
    val baseData = baseDataClick
      .join(cvrData, Seq("searchid"), "left_outer")
      .na.fill(0, Seq("iscvr"))
      .select("searchid", "unitid", "bid", "price", "exp_cvr", "isclick", "isshow", "iscvr")

    // 计算各维度下的pcoc、jfb以及后验cvr等指标
    val dataRaw1 = calculateData1(baseData, date, hour, spark)
    val data1 = dataRaw1
        .filter(s"cv >= $minCV")
        .cache()
    data1.show(10)

    // 计算该维度下根据给定highBidFactor计算出的lowBidFactor
    val baseData2 = baseData
      .join(data1, Seq("unitid"), "inner")

    baseData2.show(10)

//    val data2 = calculateData2(baseData2, highBidFactor, lowBidFactor, date, hour, spark)
//    //    data2.repartition(10).write.mode("overwrite").saveAsTable("test.check_ocpc_calibration2")
//
//    val resultDF = data1
//      .join(data2, Seq("unitid"), "inner")
//      .withColumn("high_bid_factor", lit(highBidFactor))
//      .select("unitid", "pcoc", "jfb", "post_cvr", "high_bid_factor", "low_bid_factor")
//
//    resultDF

  }

  def calculateData2(baseData: DataFrame, highBidFactor: Double, lowBidFactor: Double, date: String, hour: String, spark: SparkSession) = {
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
         |  bid,
         |  price,
         |  exp_cvr,
         |  isclick,
         |  isshow,
         |  exp_cvr * 1.0 / pcoc as pcvr,
         |  post_cvr
         |FROM
         |  base_data
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
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then pcvr else 0 end) * 1.0 / sum(isclick) as pre_cvr
         |FROM
         |  raw_data
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("calc_total", col("pre_cvr") * col("click"))
      .select("unitid", "calc_total")

    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then pcvr else 0 end) * 1.0 / sum(isclick) as pre_cvr
         |FROM
         |  raw_data
         |WHERE
         |  pcvr_group = "high"
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .withColumn("calc_high", col("pre_cvr") * col("click") * highBidFactor)
      .select("unitid", "calc_high")

    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  sum(isclick) as click,
         |  sum(case when isclick=1 then pcvr else 0 end) * 1.0 / sum(isclick) as pre_cvr
         |FROM
         |  raw_data
         |WHERE
         |  pcvr_group = "low"
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest3)
    val data3 = spark
      .sql(sqlRequest3)
      .withColumn("calc_low", col("pre_cvr") * col("click"))
      .select("unitid", "calc_low")

    val data = data1
      .join(data2, Seq("unitid"), "inner")
      .join(data3, Seq("unitid"), "inner")
      .select("unitid", "calc_total", "calc_high", "calc_low")

    data.createOrReplaceTempView("data")
    val sqlRequestFinal =
      s"""
         |SELECT
         |  unitid,
         |  calc_total,
         |  calc_high,
         |  calc_low,
         |  (calc_total - calc_high) * 1.0 / calc_low as low_bid_factor
         |FROM
         |  data
       """.stripMargin
    println(sqlRequestFinal)
    val dataFinal = spark
        .sql(sqlRequestFinal)
        .withColumn("low_bid_factor", when(col("low_bid_factor") <= lowBidFactor, lowBidFactor).otherwise(col("low_bid_factor")))

    dataFinal
  }

  def calculateData1(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  sum(iscvr) * 1.0 / sum(isclick) as post_cvr,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then bid else 0 end) * 1.0 / sum(isclick) as acb,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv
         |FROM
         |  base_data
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("jfb", col("acp") * 1.0 / col("acb"))
      .select("unitid", "post_cvr", "pre_cvr", "acp", "acb", "pcoc", "jfb", "click", "cv")

    data
  }

  def getCvrData(cvrType: String, hourInt: Int, date: String, hour: String, spark: SparkSession) = {
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
         |  label as iscvr
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  `date` >= '$date1'
         |AND
         |  cvr_goal = '$cvrType'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getBaseData(media: String, hourInt: Int, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
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
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

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
         |  isshow
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
         |  conversion_goal = $conversionGoal
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }


}
