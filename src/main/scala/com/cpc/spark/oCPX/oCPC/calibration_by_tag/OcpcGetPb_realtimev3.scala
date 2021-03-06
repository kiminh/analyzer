package com.cpc.spark.oCPX.oCPC.calibration_by_tag

import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcBIDfactor._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcCVRfactorRealtime._
import com.cpc.spark.oCPX.OcpcTools._
//import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcCalibrationBase._
//import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcCalibrationBaseRealtime._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcJFBfactor._
import com.cpc.spark.oCPX.oCPC.calibration_all.OcpcSmoothfactor._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_realtimev3 {
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

    dataRaw
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_exp_data20190912a")


    val jfbDataRaw = OcpcJFBfactorMain(date, hour, version, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    val jfbData = jfbDataRaw
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor")
      .cache()
    jfbData.show(10)
    jfbData
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_exp_data20190912b")

    val smoothDataRaw = OcpcSmoothfactorMain(date, hour, version, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    val smoothData = smoothDataRaw
      .withColumn("post_cvr", col("cvr"))
      .select("identifier", "conversion_goal", "exp_tag", "post_cvr", "smooth_factor")
      .cache()
    smoothData.show(10)

    val dataRawRealtime = OcpcCalibrationBaseRealtimeMain(date, hour, hourInt3, spark).cache()
    dataRawRealtime
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_exp_data20190912e")

    val pcocDataRaw = OcpcCVRfactorMain(date, hour, version, expTag, dataRawRealtime, hourInt1, hourInt2, hourInt3, spark)
    val pcocData = pcocDataRaw
      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
      .select("identifier", "conversion_goal", "exp_tag", "cvr_factor")
      .cache()
    pcocData.show(10)
    pcocData
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_exp_data20190912f")

    val bidFactorDataRaw = OcpcBIDfactorData(date, hour, version, expTag, bidFactorHourInt, spark)
    val bidFactorData = bidFactorDataRaw
      .select("identifier", "conversion_goal", "exp_tag", "high_bid_factor", "low_bid_factor")
      .cache()
    bidFactorData.show(10)
    bidFactorData
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_exp_data20190912h")

    val data = assemblyData(jfbData, smoothData, pcocData, bidFactorData, spark).cache()
    data.show(10)

    dataRaw.unpersist()
    dataRawRealtime.unpersist()

    // 明投单元
    val result = data
      .withColumn("cpagiven", lit(1.0))
      .withColumn("is_hidden", lit(0))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    val resultDF = result
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly_exp_alltype")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly_exp_alltype")


  }


  def assemblyData(jfbData: DataFrame, smoothData: DataFrame, pcocData: DataFrame, bidFactorData: DataFrame, spark: SparkSession) = {
    // 组装数据
    val data = pcocData
      .filter(s"cvr_factor is not null")
      .join(jfbData, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .join(smoothData, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .join(bidFactorData, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .withColumn("smooth_factor", lit(0.0))
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
      .na.fill(1.0, Seq("jfb_factor", "cvr_factor", "high_bid_factor", "low_bid_factor"))
      .na.fill(0.0, Seq("post_cvr", "smooth_factor"))

    data
  }

  def OcpcCalibrationBaseMain(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
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
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "media", "isclick", "iscvr", "bid", "price", "exp_cvr", "date", "hour")

    // 计算结果
    val result = calculateParameter(baseData, spark)

    val resultDF = result
      .select("identifier", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")


    resultDF
  }


  def OcpcCalibrationBaseRealtimeMain(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    val baseDataRaw = getRealtimeDataDelay(hourInt, date, hour, spark)
    val baseData = baseDataRaw
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "media", "isclick", "iscvr", "exp_cvr", "date", "hour")

    // 计算结果
    val result = baseData
      .filter(s"isclick=1")
      .groupBy("identifier", "conversion_goal", "media", "date", "hour")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        sum(col("exp_cvr")).alias("total_pre_cvr")
      )
      .select("identifier", "conversion_goal", "media", "click", "cv", "total_pre_cvr", "date", "hour")


    val resultDF = result
      .select("identifier", "conversion_goal", "media", "click", "cv", "total_pre_cvr", "date", "hour")


    resultDF
  }

//  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
//    val data  =rawData
//      .filter(s"isclick=1")
//      .groupBy("identifier", "conversion_goal", "media", "date", "hour")
//      .agg(
//        sum(col("isclick")).alias("click"),
//        sum(col("iscvr")).alias("cv"),
//        sum(col("exp_cvr")).alias("total_pre_cvr")
//      )
//      .select("identifier", "conversion_goal", "media", "click", "cv", "total_pre_cvr", "date", "hour")
//
//    data
//  }



  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("identifier", "conversion_goal", "media", "date", "hour")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        sum(col("bid")).alias("total_bid"),
        sum(col("price")).alias("total_price"),
        sum(col("exp_cvr")).alias("total_pre_cvr")
      )
      .select("identifier", "conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr", "date", "hour")

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
    val baseDataRaw = getBaseDataDelay(hourInt, date, hour, spark)
    val baseData = baseDataRaw
      .selectExpr("searchid", "cast(unitid as string) identifier", "conversion_goal", "media", "isshow", "isclick", "iscvr", "bid", "price", "exp_cvr", "date", "hour")


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
      .join(data1, Seq("identifier", "conversion_goal", "media"), "inner")

    val data2 = calculateData2(baseData2, date, hour, spark)

    val resultDF = data1
      .select("identifier", "conversion_goal", "media", "exp_tag", "cv", "min_cv")
      .join(data2, Seq("identifier", "conversion_goal", "media"), "inner")
      .selectExpr("identifier", "conversion_goal", "exp_tag", "high_bid_factor", "low_bid_factor", "cv", "min_cv")
      .withColumn("version", lit(version))

    resultDF

  }


}


