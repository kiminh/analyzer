package com.cpc.spark.oCPX.deepOcpc.calibration_baseline

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_pay {
  /*
  付费深度转化校准
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt
    val minCV = args(3).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt:$hourInt")

    val rawData = OcpcCalibrationFactor(date, hour, hourInt, minCV, spark)

    // 计算cvr校准系数
    val data = calculateCalibrationValue(rawData, spark)


    // 数据组装
    val resultData = assemblyData(data, spark)

    // 输出到结果表 dl_cpc.ocpc_deep_pb_data_hourly
    // 明投单元
    val resultDF = resultData
      .withColumn("cpagiven", lit(1.0))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("deep_conversion_goal", lit(3))
      .withColumn("exp_tag", concat_ws("-", col("exp_tag"), col("deep_conversion_goal")))
      .select("conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag")

    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_deep_pb_data_hourly_baseline_exp")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_pb_data_hourly_baseline_exp")

  }

  def assemblyData(rawData: DataFrame, spark: SparkSession) = {
    // 组装数据
    // set some default value
    // post_cvr: 0.0
    // smooth_factor: 0.3
    // high_bid_factor: 1.0
    // low_bid_factor: 1.0
    val data = rawData
      .selectExpr("conversion_goal", "media", "jfb_factor", "cvr_factor")
      .withColumn("post_cvr", lit(0.0))
      .withColumn("smooth_factor", lit(0.3))
      .withColumn("high_bid_factor", lit(1.0))
      .withColumn("low_bid_factor", lit(1.0))
      .select("conversion_goal", "media", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
      .na.fill(1.0, Seq("jfb_factor", "cvr_factor", "high_bid_factor", "low_bid_factor"))
      .na.fill(0.0, Seq("post_cvr", "smooth_factor"))

    val expArray = Array("v4", "v5", "v6", "v7", "v8")
    var result = data
      .select("conversion_goal", "media", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
      .withColumn("exp_tag", udfConcatStringColumn("v3")(col("media")))
      .select("conversion_goal", "media", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "exp_tag")

    for (item <- expArray) {
      val singleData = data
        .select("conversion_goal", "media", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
        .withColumn("exp_tag", udfConcatStringColumn(item)(col("media")))
        .select("conversion_goal", "media", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "exp_tag")

      result = result.union(singleData)
    }

    result
  }

  def udfConcatStringColumn(value1: String) = udf((value2: String) => {
    val result = value1 + value2
    result
  })


  def calculateCalibrationValue(dataRaw: DataFrame, spark: SparkSession) = {
    val data = dataRaw
      .filter(s"cv >= min_cv")
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .withColumn("cvr_factor", col("post_cvr") * 1.0 / col("pre_cvr"))
      .withColumn("conversion_goal", col("deep_conversion_goal"))
      .cache()

    data
  }

  def getBaseData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
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
         |  media_appsid,
         |  (case
         |      when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |      when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |      when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |      when adclass in (110110100, 125100100) then "wzcp"
         |      else "others"
         |  end) as industry,
         |  conversion_goal,
         |  deep_conversion_goal,
         |  expids,
         |  exptags,
         |  ocpc_expand,
         |  hidden_tax,
         |  deep_cvr * 1.0 / 1000000 as exp_cvr,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  is_deep_ocpc = 1
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
         |AND
         |  deep_cvr is not null
         |AND
         |  deep_conversion_goal = 3
       """.stripMargin
    println(sqlRequest)
    val clickDataRaw = spark
      .sql(sqlRequest)
      .withColumn("bid", udfCalculateBidWithHiddenTax()(col("date"), col("bid"), col("hidden_tax")))
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))

    val clickData = mapMediaName(clickDataRaw, spark)

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  deep_conversion_goal
         |FROM
         |  dl_cpc.ocpc_label_deep_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()

    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "deep_conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }


  def OcpcCalibrationFactor(date: String, hour: String, hourInt: Int, minCV: Int, spark: SparkSession) = {
    val baseData = getBaseData(hourInt, date, hour, spark)

    val resultDF = baseData
      .filter(s"isclick=1")
      .groupBy("deep_conversion_goal", "media")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        sum(col("bid")).alias("total_bid"),
        sum(col("price")).alias("total_price"),
        sum(col("exp_cvr")).alias("total_pre_cvr")
      )
      .select( "deep_conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr")
      .na.fill(0, Seq("cv"))
      .filter(s"deep_conversion_goal = 3")
      .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("min_cv", lit(minCV))

    resultDF.show(10)

    resultDF
  }




}
