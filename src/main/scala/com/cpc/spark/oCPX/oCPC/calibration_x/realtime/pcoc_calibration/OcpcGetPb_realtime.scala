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

  def getRealtimeData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
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
    val selectCondition = getTimeRangeSqlDay(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  adslot_type,
         |  adclass,
         |  cast(exp_cvr * 1.0 / 1000000 as double) as exp_cvr,
         |  media_appsid,
         |  conversion_goal,
         |  isclick
         |FROM
         |  dl_cpc.cpc_basedata_click_event
         |WHERE
         |  $selectCondition
         |AND
         |  ocpc_step >= 1
         |AND
         |  adslot_type != 7
         |AND
         |  isclick = 1
         |AND
         |  antispam_score = 10000
       """.stripMargin
    println(sqlRequest)
    val clickDataRaw = spark
      .sql(sqlRequest)

    val clickData = mapMediaName(clickDataRaw, spark)

    spark.udf.register("getConversionGoal", (traceType: String, traceOp1: String, traceOp2: String) => {
      var result = (traceType, traceOp1, traceOp2) match {
        case (_, "REPORT_DOWNLOAD_PKGADDED", _) => 1
        case ("active_third", _, "") | ("active_third", _, "0") => 2
        case ("active_third", _, "26") | ("active15", _, "site_form") | ("ctsite_active15", _, "ct_site_form") | ("js_active", _, "js_form") => 3
        case (_, "REPORT_USER_STAYINWX", _) | ("js_active", _, "active_copywx") | (_, "REPORT_ICON_STAYINWX", "ON_BANNER") | (_, "REPORT_ICON_STAYINWX", "CLICK_POPUPWINDOW_ADDWX") => 4
        case ("active_third", _, "1") => 5
        case (_, _, _) => 0
      }
      result
    })

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  trace_type,
         |  trace_op1,
         |  trace_op2,
         |  getConversionGoal(trace_type, trace_op1, trace_op2) as conversion_goal
         |FROM
         |  dl_cpc.cpc_basedata_trace_event
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark
      .sql(sqlRequest2)
      .select("searchid", "conversion_goal")
      .filter(s"conversion_goal > 0")
      .withColumn("iscvr", lit(1))
      .distinct()

    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

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


