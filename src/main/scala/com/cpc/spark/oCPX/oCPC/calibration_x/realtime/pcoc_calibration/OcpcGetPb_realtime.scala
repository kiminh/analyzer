package com.cpc.spark.oCPX.oCPC.calibration_x.realtime.pcoc_calibration

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcBIDfactor.{calculateData1, calculateData2}
import com.cpc.spark.oCPX.oCPC.calibration_by_tag.OcpcGetPb_baseline.calculateParameter
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_realtime {
  /*
  新增部分媒体id采用暗投
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val date1 = args(2).toString
    val hour1 = args(3).toString
    val version = args(4).toString
    val expTag = args(5).toString
    val hourInt = args(6).toInt
    val minCV = args(7).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, date1:$date1, hour1:$hour1, version:$version, expTag:$expTag, hourInt:$hourInt, minCV:$minCV")

    // 计算jfb_factor,
    val jfbData = OcpcJFBfactor(date, hour, spark)

    // 计算pcoc
    val pcocData = OcpcCVRfactor(date1, hour1, hourInt, minCV, spark)

    // 分段校准
    val bidFactor = OcpcBIDfactor(date, hour, version, expTag, 48, spark)

    val data = assemblyData(jfbData, pcocData, bidFactor, expTag, spark)

    val result = data
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
      .withColumn("cpagiven", lit(1.0))
      .withColumn("is_hidden", lit(0))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .selectExpr("identifier", "cast(identifier as int) unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    val resultDF = result
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly_exp")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly_exp")
//

  }

  def assemblyData(jfbData: DataFrame, pcocData: DataFrame, bidFactor: DataFrame, expTag: String, spark: SparkSession) = {
    // 组装数据
    val result = pcocData
      .join(jfbData, Seq("identifier", "conversion_goal", "media"), "inner")
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(bidFactor, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor", "cvr_factor", "post_cvr", "high_bid_factor", "low_bid_factor")
      .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor"))
      .na.fill(0.0, Seq("post_cvr"))
      .withColumn("smooth_factor", lit(0.0))

    result
  }

  /*
  分段校准
   */
  def OcpcBIDfactor(date: String, hour: String, version: String, expTag: String, hourInt: Int, spark:SparkSession) = {
    /*
    计算新版的cvr平滑策略：
    1. 抽取基础数据
    2. 计算该维度下pcoc与计费比、后验cvr等等指标
    3. 计算该维度下根据给定highBidFactor计算出的lowBidFactor
     */

    // 抽取基础数据
    val baseDataRaw = getBaseData(hourInt, date, hour, spark)
    baseDataRaw
      .withColumn("bid", udfCalculateBidWithHiddenTax()(col("date"), col("bid"), col("hidden_tax")))
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))
      .createOrReplaceTempView("base_data_raw")

    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  base_data_raw
       """.stripMargin
    println(sqlRequest)
    val baseData = spark
      .sql(sqlRequest)
      .selectExpr("searchid", "cast(unitid as string) identifier", "conversion_goal", "media", "isshow", "isclick", "iscvr", "bid", "price", "exp_cvr", "date", "hour")


    // 计算各维度下的pcoc、jfb以及后验cvr等指标
    val dataRaw1 = calculateData1(baseData, version, expTag, date, hour, spark)

    val data1 = dataRaw1
      .filter(s"cv >= min_cv")
      .cache()
    data1.show(10)


    // 计算该维度下根据给定highBidFactor计算出的lowBidFactor
    val baseData2 = baseData
      .withColumn("media", udfMediaName()(col("media")))
      .join(data1, Seq("identifier", "conversion_goal", "media"), "inner")

    val data2 = calculateData2(baseData2, date, hour, spark)

    val resultDF = data1
      .select("identifier", "conversion_goal", "media", "exp_tag", "cv", "min_cv", "post_cvr")
      .join(data2, Seq("identifier", "conversion_goal", "media"), "inner")
      .selectExpr("identifier", "conversion_goal", "exp_tag", "high_bid_factor", "low_bid_factor", "post_cvr", "cv", "min_cv")
      .withColumn("version", lit(version))

    resultDF

  }

  /*
  cvr校准系数
   */
  def OcpcCVRfactor(date: String, hour: String, hourInt: Int, minCV: Int, spark: SparkSession) = {
    val baseDataRaw = getRealtimeData(24, date, hour, spark)
    baseDataRaw.createOrReplaceTempView("base_data_raw")

    val sqlRequest =
      s"""
         |SELECT
         |  cast(unitid as string) identifier,
         |  userid,
         |  conversion_goal,
         |  media,
         |  sum(exp_cvr) as total_pre_cvr,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv
         |FROM
         |  base_data_raw
         |WHERE
         |  isclick=1
         |GROUP BY cast(unitid as string), userid, conversion_goal, media
       """.stripMargin
    println(sqlRequest)

    val baseData = spark
      .sql(sqlRequest)
      .select("identifier", "userid", "conversion_goal", "media", "total_pre_cvr", "click", "cv")
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("cvr_factor", col("post_cvr") * 1.0 / col("pre_cvr"))

    val resultDF = baseData
      .select("identifier", "userid", "conversion_goal", "media", "total_pre_cvr", "click", "cv", "cvr_factor")
      .withColumn("cvr_factor", udfCheckBound(0.5, 2.0)(col("cvr_factor")))
      .filter(s"cv > $minCV and cvr_factor is not null")

    resultDF
  }


  /*
  计费比系数
   */
  def OcpcJFBfactor(date: String, hour: String, spark: SparkSession) = {
    val baseDataRaw = getBaseData(24, date, hour, spark)
    baseDataRaw
      .withColumn("bid", udfCalculateBidWithHiddenTax()(col("date"), col("bid"), col("hidden_tax")))
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))
      .createOrReplaceTempView("base_data_raw")

    val sqlRequest =
      s"""
         |SELECT
         |  cast(unitid as string) identifier,
         |  userid,
         |  conversion_goal,
         |  media,
         |  sum(isclick) as click,
         |  avg(bid) as acb,
         |  avg(price) as acp
         |FROM
         |  base_data_raw
         |WHERE
         |  isclick=1
         |GROUP BY cast(unitid as string), userid, conversion_goal, media
       """.stripMargin
    println(sqlRequest)
    val baseData = spark
      .sql(sqlRequest)
      .select("identifier", "userid", "conversion_goal", "media", "click", "acb", "acp")
      .withColumn("jfb_factor", col("acb") * 1.0 / col("acp") )

    val resultDF = baseData
      .select("identifier", "userid", "conversion_goal", "media", "click", "acb", "acp", "jfb_factor")
      .withColumn("jfb_factor", udfCheckBound(1.0, 3.0)(col("jfb_factor")))
      .filter(s"jfb_factor is not null")

    resultDF
  }

  def udfCheckBound(minValue: Double, maxValue: Double) = udf((jfbFactor: Double) => {
    var result = math.min(math.max(minValue, jfbFactor), maxValue)
    result
  })


}


