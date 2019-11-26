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
    val bidFactorHourInt = args(4).toInt

    // 主校准回溯时间长度
    val hourInt1 = args(5).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(6).toInt
    // 兜底校准时长
    val hourInt3 = args(7).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt1:$hourInt1, hourInt2:$hourInt2, hourInt3:$hourInt3")

    // 计算jfb_factor,
    val jfbDataRaw = OcpcJFBfactor(date, hour, spark)

//
//    // 计算pcoc
//    val pcocDataRaw = OcpcCVRfactorMain(date, hour, version, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
//    val pcocData = pcocDataRaw
//      .withColumn("cvr_factor", lit(1.0) / col("pcoc"))
//      .select("identifier", "conversion_goal", "exp_tag", "cvr_factor")
//      .cache()
//    pcocData.show(10)
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
         |  avg(acp) as acp
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

    resultDF
  }


}


