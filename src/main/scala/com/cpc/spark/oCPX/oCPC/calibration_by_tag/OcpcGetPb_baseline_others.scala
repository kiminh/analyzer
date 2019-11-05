package com.cpc.spark.oCPX.oCPC.calibration_by_tag

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcBIDfactor._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcCVRfactorRealtime._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcCalculateCalibrationValue._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcJFBfactor._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcSmoothfactor._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_baseline_others {
  /*
  新增部分媒体id采用暗投
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    val bidFactorHourInt = args(4).toInt

    // 主校准回溯时间长度
    val hourInt1 = args(5).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(6).toInt
    // 兜底校准时长
    val hourInt3 = args(7).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    // 计算jfb_factor,cvr_factor,post_cvr
    val dataRaw = OcpcCalibrationBaseMain(date, hour, hourInt3, spark).cache()
    dataRaw.show(10)

    // 计算兜底校准系数:jfb_factor, post_cvr, cvr_factor
    val dataRaw1 = dataRaw
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("identifier", concat_ws("-", col("userid"), col("conversion_goal"), col("exp_tag")))
      .select("identifier", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")
    val useridResult = OcpcCalculateCalibrationValueMain(dataRaw1, 40, spark)
    val dataRaw2 = dataRaw
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("identifier", concat_ws("-", col("conversion_goal"), col("exp_tag")))
      .select("identifier", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")
    val cvgoalResult = OcpcCalculateCalibrationValueMain(dataRaw2, 0, spark)

    val jfbDataRaw = OcpcJFBfactorMain(date, hour, version, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    val jfbData = jfbDataRaw
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor")
      .cache()
    jfbData.show(10)

    val smoothDataRaw = OcpcSmoothfactorMain(date, hour, version, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    val smoothData = smoothDataRaw
      .withColumn("post_cvr", col("cvr"))
      .select("identifier", "conversion_goal", "exp_tag", "post_cvr", "smooth_factor")
      .cache()
    smoothData.show(10)

    val pcocDataRaw = OcpcCVRfactorMain(date, hour, version, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    val pcocData = pcocDataRaw
      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
      .select("identifier", "conversion_goal", "exp_tag", "cvr_factor")
      .cache()
    pcocData.show(10)

    val bidFactorDataRaw = OcpcBIDfactorData(date, hour, version, expTag, bidFactorHourInt, spark)
    val bidFactorData = bidFactorDataRaw
      .select("identifier", "conversion_goal", "exp_tag", "high_bid_factor", "low_bid_factor")
      .cache()
    bidFactorData.show(10)

    val data = assemblyData(jfbData, smoothData, pcocData, bidFactorData, useridResult, cvgoalResult, dataRaw, expTag, spark).cache()
    data.show(10)

    dataRaw.unpersist()

    // 明投单元
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
      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly_exp")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly_exp")


  }


  def assemblyData(jfbData: DataFrame, smoothData: DataFrame, pcocData: DataFrame, bidFactorData: DataFrame, useridResultRaw: DataFrame, cvgoalResultRaw: DataFrame, baseDataRaw: DataFrame, expTag: String, spark: SparkSession) = {
    // 组装数据
    val caliData = pcocData
      .join(jfbData, Seq("identifier", "conversion_goal", "exp_tag"), "inner")
      .join(smoothData, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .join(bidFactorData, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
      .na.fill(1.0, Seq("jfb_factor", "high_bid_factor", "low_bid_factor"))
      .na.fill(0.0, Seq("post_cvr", "smooth_factor"))
      .select(
        col("identifier"),
        col("conversion_goal"),
        col("exp_tag"),
        col("jfb_factor").alias("jfb_factor_cali"),
        col("post_cvr").alias("post_cvr_cali"),
        col("smooth_factor"),
        col("cvr_factor").alias("cvr_factor_cali"),
        col("high_bid_factor"),
        col("low_bid_factor")
      )

    // 主要数据：最近84小时有记录
    val baseData = baseDataRaw
        .select("identifier", "userid", "conversion_goal", "media")
        .withColumn("media", udfMediaName()(col("media")))
        .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
        .select("identifier", "userid", "conversion_goal", "exp_tag")
        .distinct()

    val useridResult = useridResultRaw
        .withColumn("id", split(col("identifier"), "-"))
        .select(
          col("id").getItem(0).as("userid"),
          col("id").getItem(1).as("conversion_goal"),
          col("id").getItem(2).as("exp_tag"),
          col("jfb_factor").alias("jfb_factor1"),
          col("post_cvr").alias("post_cvr1"),
          col("cvr_factor").alias("cvr_factor1")
        )
        .selectExpr("cast(userid as int) userid", "cast(conversion_goal as int) conversion_goal", "cast(exp_tag as string) exp_tag", "jfb_factor1", "cvr_factor1", "post_cvr1")

    val cvgoalResult = cvgoalResultRaw
      .withColumn("id", split(col("identifier"), "-"))
      .select(
          col("id").getItem(0).as("conversion_goal"),
          col("id").getItem(1).as("exp_tag"),
          col("jfb_factor").alias("jfb_factor2"),
          col("post_cvr").alias("post_cvr2"),
          col("cvr_factor").alias("cvr_factor2")
        )
        .selectExpr("cast(conversion_goal as int) conversion_goal", "cast(exp_tag as string) exp_tag", "jfb_factor2", "cvr_factor2", "post_cvr2")

    // 兜底校准系数
    val bottomData = baseData
        .join(useridResult, Seq("userid", "conversion_goal", "exp_tag"), "left_outer")
        .join(cvgoalResult, Seq("conversion_goal", "exp_tag"), "left_outer")
        .na.fill(-1.0, Seq("jfb_factor1", "cvr_factor1", "post_cvr1", "jfb_factor2", "cvr_factor2", "post_cvr2"))
        .withColumn("jfb_factor_base", udfBottomValue()(col("jfb_factor1"), col("jfb_factor2")))
        .withColumn("cvr_factor_base", udfBottomValue()(col("cvr_factor1"), col("cvr_factor2")))
        .withColumn("post_cvr_base", udfBottomValue()(col("post_cvr1"), col("post_cvr2")))

    // 数据关联
    val data = bottomData
        .join(caliData, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
        .na.fill(-1.0, Seq("jfb_factor_cali", "cvr_factor_cali"))
        .na.fill(0.0, Seq("smooth_factor", "post_cvr_cali"))
        .na.fill(1.0, Seq("high_bid_factor", "low_bid_factor"))
        .withColumn("jfb_factor", udfBottomValue()(col("jfb_factor_cali"), col("jfb_factor_base")))
        .withColumn("cvr_factor", udfBottomValue()(col("cvr_factor_cali"), col("cvr_factor_base")))
        .withColumn("post_cvr", col("post_cvr_cali"))


    data

  }

  def udfBottomValue() = udf((value1: Double, value2: Double) => {
    var result = value1
    if (value1 <= 0.0) {
      result = value2
    }
    result
  })

  def getBaseDataDelayOther(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
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
         |  unitid,
         |  userid,
         |  adslot_type,
         |  isshow,
         |  isclick,
         |  bid_discounted_by_ad_slot as bid,
         |  price,
         |  cast(exp_cvr as double) as exp_cvr,
         |  cast(exp_ctr as double) as exp_ctr,
         |  media_appsid,
         |  (case
         |      when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |      when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |      when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |      when adclass in (110110100, 125100100) then "wzcp"
         |      else "others"
         |  end) as industry,
         |  conversion_goal,
         |  expids,
         |  exptags,
         |  ocpc_expand,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("media", lit("Other"))

    // 抽取cv数据
    val sqlRequest2 =
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
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()


    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }


  def OcpcCalibrationBaseMainOther(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseDataRaw = getBaseDataDelay(hourInt, date, hour, spark)
    baseDataRaw.createOrReplaceTempView("base_data_raw")

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
      .selectExpr("cast(unitid as string) identifier", "userid", "conversion_goal", "media", "isclick", "iscvr", "bid", "price", "exp_cvr", "date", "hour")



    // 计算结果
    val result = calculateParameter(baseData, spark)

    val resultDF = result
      .select("identifier", "userid", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")


    resultDF
  }


  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("identifier", "userid", "conversion_goal", "media", "date", "hour")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        sum(col("bid")).alias("total_bid"),
        sum(col("price")).alias("total_price"),
        sum(col("exp_cvr")).alias("total_pre_cvr")
      )
      .select("identifier", "userid", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")

    data
  }

  def OcpcBIDfactorData(date: String, hour: String, version: String, expTag: String, hourInt: Int, spark:SparkSession) = {
    /*
    计算新版的cvr平滑策略：
    1. 抽取基础数据
    2. 计算该维度下pcoc与计费比、后验cvr等等指标
    3. 计算该维度下根据给定highBidFactor计算出的lowBidFactor
     */

    // 抽取基础数据
    val baseDataRaw = getBaseDataDelayOther(hourInt, date, hour, spark)
    baseDataRaw.createOrReplaceTempView("base_data_raw")

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
      .selectExpr("identifier", "conversion_goal", "exp_tag", "high_bid_factor", "low_bid_factor", "cv", "min_cv")
      .withColumn("version", lit(version))

    resultDF

  }

}


