package com.cpc.spark.oCPX.deepOcpc.calibration_v5.rentention

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfDetermineMedia, udfMediaName, udfSetExpTag}
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcRetentionFactor._
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcShallowFactor._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcDeepBase_shallowfactor {
  /*
  采用基于后验激活率的复合校准策略
  jfb_factor：正常计算
  cvr_factor：
  cvr_factor = (deep_cvr * post_cvr1) / pre_cvr1
  smooth_factor = 0.3
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val expTag = args(2).toString
    val minCV = args(3).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, expTag=$expTag, minCV=$minCV")

    // 计算分小时数据基础数据表
    val baseData = calculateBaseData(date, hour, expTag, spark)

    // 按照minCV过滤出合适的
    val result = calculateCalibration(baseData, minCV, spark)

    baseData
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191118a")

    result
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191118b")

  }

  def calculateCalibration(rawData: DataFrame, minCV: Int, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  conversion_goal as deep_conversion_goal,
         |  (case when hour_diff = 24 then 1
         |        when hour_diff = 48 then 2
         |        when hour_diff = 72 then 3
         |        else 0
         |   end) as time_window,
         |   value as recall_value
         |FROM
         |  dl_cpc.algo_recall_info_v1
         |WHERE
         |  version = 'v1'
         |""".stripMargin
    println(sqlRequest)
    val recallValue = spark.sql(sqlRequest)

    val baseData = rawData
      .join(recallValue, Seq("deep_conversion_goal", "time_window"), "left_outer")
      .na.fill(1.0, Seq("recall_value"))
      .withColumn("recall_cv", col("cv") * col("recall_value"))

    val base1 = baseData
      .filter(s"time_window = 1")
      .select("unitid", "deep_conversion_goal", "media", "click1", "cv1", "total_pre_cvr1", "recall_cv1")

    val base2 = baseData
      .filter(s"time_window = 2")
      .select("unitid", "deep_conversion_goal", "media", "click2", "cv2", "total_pre_cvr2", "recall_cv2")

    val base3 = baseData
      .filter(s"time_window = 3")
      .select("unitid", "deep_conversion_goal", "media", "click3", "cv3", "total_pre_cvr3", "recall_cv3")

    val resultDF = base1
      .join(base2, Seq("unitid", "deep_conversion_goal", "media"), "outer")
      .join(base3, Seq("unitid", "deep_conversion_goal", "media"), "outer")
      .na.fill(0, Seq("click1", "cv1", "recall_cv1", "total_pre_cvr1", "click2", "cv2", "recall_cv2", "total_pre_cvr2", "click3", "cv3", "recall_cv3", "total_pre_cvr3"))
      .withColumn("window_length", udfDetermineWindowLength(minCV)(col("cv1"), col("cv2"), col("cv3")))
      .withColumn("recall_cv", udfDetermineValueByWindow()(col("window_length"), col("recall_cv1"), col("recall_cv2"), col("recall_cv3")))
      .withColumn("total_pre_cvr", udfDetermineValueByWindow()(col("window_length"), col("total_pre_cvr1"), col("total_pre_cvr2"), col("total_pre_cvr3")))
      .withColumn("click", udfDetermineValueByWindow()(col("window_length"), col("click1"), col("click2"), col("click3")))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("shallow_factor", col("post_cvr") * 1.0 / col("pre_cvr"))
      .cache()

    resultDF.show(10)
    resultDF
  }

  def udfDetermineValueByWindow() = udf((windowLength: Int, value1: Double, value2: Double, value3: Double) => {
    var result = windowLength match {
      case 0 => 0
      case 1 => value1
      case 2 => value1 + value2
      case 3 => value1 + value2 + value3
    }
    result
  })

  def calculateBaseData(date: String, hour: String, expTag: String, spark: SparkSession) = {
    val rawData = getShallowBaseData(72, date, hour, spark)

    val resultDF = rawData
      .filter(s"isclick=1")
      .withColumn("time_window", udfLabelTimeWindow(date, hour)(col("date"), col("hour")))
      .groupBy("unitid", "deep_conversion_goal", "media", "time_window")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        sum(col("exp_cvr")).alias("total_pre_cvr")
      )
      .select("unitid", "deep_conversion_goal", "media", "time_window", "click", "cv", "total_pre_cvr")
      .na.fill(0, Seq("cv"))
      .filter(s"deep_conversion_goal = 2")
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .cache()


    resultDF.show(10)

    resultDF
  }

  def udfDetermineWindowLength(minCV: Int) = udf((cv1: Int, cv2: Int, cv3: Int) => {
    val result = {
      if (cv1 >= minCV) {
        1
      } else if (cv1 + cv2 >= minCV) {
        2
      } else if (cv1 + cv2 + cv3 >= minCV) {
        3
      } else {
        0
      }
    }
    result
  })

  def udfLabelTimeWindow(date: String, hour: String) = udf((dateCol: String, hourCol: String) => {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val time0 = date + " " + hour
    val today = dateConverter.parse(time0)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    val epochTime0 = calendar.getTimeInMillis
    calendar.add(Calendar.DATE, -1)
    val epochTime1 = calendar.getTimeInMillis
    calendar.add(Calendar.DATE, -1)
    val epochTime2 = calendar.getTimeInMillis

    val time = dateCol + " " + hourCol
    val epochTime = dateConverter.parse(time).getTime()

    var result = {
      if (epochTime > epochTime1) {
        1
      } else if (epochTime > epochTime2) {
        2
      } else {
        3
      }
    }
    result

  })

  def getShallowBaseData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
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
         |  exp_cvr,
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
         |  deep_conversion_goal = 2
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
         |  conversion_goal
         |FROM
         |  dl_cpc.ocpc_cvr_log_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()

    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "conversion_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }


}
