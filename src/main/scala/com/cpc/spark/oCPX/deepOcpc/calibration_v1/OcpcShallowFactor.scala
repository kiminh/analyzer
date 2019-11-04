package com.cpc.spark.oCPX.deepOcpc.calibration_v1

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcCalibrationBase._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcShallowFactor {
  def main(args: Array[String]): Unit = {
    /*
    计算计费比
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val expTag = args(2).toString
    val hourInt = args(3).toInt
    val minCV = args(4).toInt

    // 实验数据
    println("parameters:")
    println(s"date=$date, hour=$hour, expTag=$expTag, hourInt=$hourInt")

    val result = OcpcShallowFactorMain(date, hour, hourInt, expTag, minCV, spark).cache()

    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_ocpc_factor20191029a")
  }

  def OcpcShallowFactorMain(date: String, hour: String, hourInt: Int, expTag: String, minCV: Int, spark: SparkSession) = {
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
      .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("min_cv", lit(minCV))

    resultDF.show(10)

    resultDF
  }

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
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  conversion_goal
         |FROM
         |  dl_cpc.ocpc_cvr_log_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2)

    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }

}