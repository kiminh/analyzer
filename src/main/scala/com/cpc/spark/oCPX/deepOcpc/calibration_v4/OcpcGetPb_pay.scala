package com.cpc.spark.oCPX.deepOcpc.calibration_v4

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfDetermineMedia, udfMediaName, udfSetExpTag}
import com.typesafe.config.ConfigFactory
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
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = args(4).toInt
    val minCV = args(5).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt:$hourInt")

    val rawData = OcpcCalibrationFactor(date, hour, hourInt, expTag, minCV, spark)
//    rawData
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191029a")

    // 计算cvr校准系数
    val data = calculateCalibrationValue(rawData, spark)
//    data
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191029c")


    // 数据组装
    val resultData = assemblyData(data, spark)

    // 输出到结果表 dl_cpc.ocpc_deep_pb_data_hourly
    // 明投单元
    val result = resultData
      .withColumn("cpagiven", lit(1.0))
      .withColumn("is_hidden", lit(0))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    val resultDF = result
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")



    resultDF
      .withColumn("deep_conversion_goal", lit(3))
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_deep_pb_data_hourly_exp")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_pb_data_hourly_exp")

  }

  def assemblyData(rawData: DataFrame, spark: SparkSession) = {
    // 组装数据
    // set some default value
    // post_cvr: 0.0
    // smooth_factor: 0.3
    // high_bid_factor: 1.0
    // low_bid_factor: 1.0
    val data = rawData
      .selectExpr("cast(unitid as string) identifier", "conversion_goal", "exp_tag", "jfb_factor", "cvr_factor")
      .withColumn("post_cvr", lit(0.0))
      .withColumn("smooth_factor", lit(0.3))
      .withColumn("smooth_factor", udfSetSmoothFactor()(col("identifier"), col("smooth_factor")))
      .withColumn("high_bid_factor", lit(1.0))
      .withColumn("low_bid_factor", lit(1.0))
      .select("identifier", "conversion_goal", "exp_tag", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor")
      .na.fill(1.0, Seq("jfb_factor", "cvr_factor", "high_bid_factor", "low_bid_factor"))
      .na.fill(0.0, Seq("post_cvr", "smooth_factor"))

    data
  }


  def calculateCalibrationValue(dataRaw: DataFrame, spark: SparkSession) = {
    val data = dataRaw
      .filter(s"cv >= min_cv")
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .withColumn("cvr_factor", col("post_cvr") * 1.0 / col("pre_cvr"))
      .withColumn("conversion_goal", col("deep_conversion_goal"))
      .cache()

    data
  }

  def udfSetSmoothFactor() = udf((identifier: String, smoothFactor: Double) => {
    val result = (identifier, smoothFactor) match {
      case ("2399667", _) => 0.7
      case (_, v) => v
    }
    result
  })

  def getBaseData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
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
         |  (case when hidden_tax is null then 0 else hidden_tax end) as hidden_tax,
         |  deep_cvr * 1.0 / 1000000 as exp_cvr,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
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
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("price", col("price") - col("hidden_tax"))

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


  def OcpcCalibrationFactor(date: String, hour: String, hourInt: Int, expTag: String, minCV: Int, spark: SparkSession) = {
    val baseData = getBaseData(hourInt, date, hour, spark)

    val resultDF = baseData
      .filter(s"isclick=1")
      .groupBy("unitid", "deep_conversion_goal", "media")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        sum(col("bid")).alias("total_bid"),
        sum(col("price")).alias("total_price"),
        sum(col("exp_cvr")).alias("total_pre_cvr")
      )
      .select("unitid", "deep_conversion_goal", "media", "click", "cv", "total_bid", "total_price", "total_pre_cvr")
      .na.fill(0, Seq("cv"))
      .filter(s"deep_conversion_goal = 3")
      .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("min_cv", lit(minCV))

    resultDF.show(10)

    resultDF
  }




}
