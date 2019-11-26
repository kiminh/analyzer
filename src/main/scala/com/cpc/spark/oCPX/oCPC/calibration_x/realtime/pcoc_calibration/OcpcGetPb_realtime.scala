package com.cpc.spark.oCPX.oCPC.calibration_x.realtime.pcoc_calibration

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
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
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = args(4).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt:$hourInt")

    // 计算jfb_factor,
    val jfbData = OcpcJFBfactor(date, hour, spark)

    // 计算pcoc
    val pcocData = OcpcCVRfactor(date, hour, hourInt, spark)
//
//    // 计算分段校准
//    val bidFactorDataRaw = OcpcBIDfactorDataOther(date, hour, version, expTag, bidFactorHourInt, spark)
//    val bidFactorData = bidFactorDataRaw
//      .select("identifier", "conversion_goal", "exp_tag", "high_bid_factor", "low_bid_factor")
//      .cache()
//    bidFactorData.show(10)
//
//    val data = assemblyData(jfbData, smoothData, pcocData, bidFactorData, useridResult, cvgoalResult, dataRaw, expTag, spark).cache()
//    data.show(10)
//
//    dataRaw.unpersist()
//
//    // 明投单元
//    val result = data
//      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
//      .withColumn("cpagiven", lit(1.0))
//      .withColumn("is_hidden", lit(0))
//      .withColumn("date", lit(date))
//      .withColumn("hour", lit(hour))
//      .withColumn("version", lit(version))
//      .selectExpr("identifier", "cast(identifier as int) unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")
//
//    val resultDF = result
//      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")
//
//    resultDF
//      .repartition(1)
////      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly_exp")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly_exp")
//

  }

  /*
  cvr校准系数
   */
  def OcpcCVRfactor(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
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

    resultDF
  }

  def udfCheckBound(minValue: Double, maxValue: Double) = udf((jfbFactor: Double) => {
    var result = math.min(math.max(minValue, jfbFactor), maxValue)
    result
  })


}


