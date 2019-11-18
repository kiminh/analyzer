package com.cpc.spark.oCPX.deepOcpc.calibration_v5.retention

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDay, udfDetermineMedia, udfMediaName, udfSetExpTag}
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcRetentionFactor._
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcShallowFactor._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcDeepBase_deepfactor {
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
    val expTag = args(1).toString
    val minCV = args(2).toInt

    println("parameters:")
    println(s"date=$date, expTag:$expTag, minCV:$minCV")

    // 计算计费比系数、后验激活转化率、先验点击次留率
    val baseData = calculateBaseData(date, spark)

    // 计算自然天激活次留率
    val result = calculateCalibration(baseData, minCV, spark)

    baseData
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191118a")

    result
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191118b")


  }


  def calculateCalibration(rawData: DataFrame, minCV: Int, spark: SparkSession) = {
    val base1 = rawData
      .filter(s"time_window = 1")
      .groupBy("unitid", "deep_conversion_goal", "media")
      .agg(
        sum(col("click")).alias("click1"),
        sum(col("total_exp_cvr")).alias("total_exp_cvr1"),
        sum(col("total_retention_cvr")).alias("total_retention_cvr1"),
        sum(col("activation_cv")).alias("activation_cv1"),
        sum(col("retention_cv")).alias("retention_cv1")
      )
      .select("unitid", "deep_conversion_goal", "media", "click1", "total_exp_cvr1", "total_retention_cvr1", "activation_cv1", "retention_cv1")

    val base2 = rawData
      .filter(s"time_window = 2")
      .groupBy("unitid", "deep_conversion_goal", "media")
      .agg(
        sum(col("click")).alias("click2"),
        sum(col("total_exp_cvr")).alias("total_exp_cvr2"),
        sum(col("total_retention_cvr")).alias("total_retention_cvr2"),
        sum(col("activation_cv")).alias("activation_cv2"),
        sum(col("retention_cv")).alias("retention_cv2")
      )
      .select("unitid", "deep_conversion_goal", "media", "click2", "total_exp_cvr2", "total_retention_cvr2", "activation_cv2", "retention_cv2")

    val base3 = rawData
      .filter(s"time_window = 2")
      .groupBy("unitid", "deep_conversion_goal", "media")
      .agg(
        sum(col("click")).alias("click3"),
        sum(col("total_exp_cvr")).alias("total_exp_cvr3"),
        sum(col("total_retention_cvr")).alias("total_retention_cvr3"),
        sum(col("activation_cv")).alias("activation_cv3"),
        sum(col("retention_cv")).alias("retention_cv3")
      )
      .select("unitid", "deep_conversion_goal", "media", "click3", "total_exp_cvr3", "total_retention_cvr3", "activation_cv3", "retention_cv3")

    val resultDF = base1
      .join(base2, Seq("unitid", "deep_conversion_goal", "media"), "outer")
      .join(base3, Seq("unitid", "deep_conversion_goal", "media"), "outer")
      .na.fill(0, Seq("click1", "total_exp_cvr1", "total_retention_cvr1", "activation_cv1", "retention_cv1", "click2", "total_exp_cvr2", "total_retention_cvr2", "activation_cv2", "retention_cv2", "click3", "total_exp_cvr3", "total_retention_cvr3", "activation_cv3", "retention_cv3"))
      .withColumn("window_length", udfDetermineWindowLength(minCV)(col("retention_cv1"), col("retention_cv2"), col("retention_cv3")))
      .withColumn("click", udfDetermineValueByWindow()(col("window_length"), col("click1"), col("click2"), col("click3")))
      .withColumn("activation_cv", udfDetermineValueByWindow()(col("window_length"), col("activation_cv1"), col("activation_cv2"), col("activation_cv3")))
      .withColumn("retention_cv", udfDetermineValueByWindow()(col("window_length"), col("retention_cv1"), col("retention_cv2"), col("retention_cv3")))
      .withColumn("total_exp_cvr", udfDetermineValueByWindow()(col("window_length"), col("total_exp_cvr1"), col("total_exp_cvr2"), col("total_exp_cvr3")))
      .withColumn("total_retention_cvr", udfDetermineValueByWindow()(col("window_length"), col("total_retention_cvr1"), col("total_retention_cvr2"), col("total_retention_cvr3")))
      .withColumn("deep_cvr", col("retention_cv") * 1.0 / col("activation_cv"))
      .withColumn("exp_cvr", col("total_exp_cvr") * 1.0 / col("click"))
      .withColumn("retention_cvr", col("total_retention_cvr") * 1.0 / col("click"))
      .withColumn("deep_factor", (col("exp_cvr") * col("deep_cvr")) * 1.0 / col("retention_cvr"))
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

  def calculateBaseData(date: String, spark: SparkSession) = {
    val postCvrData = getPostCvData(date, 3, spark)
    val preCvrData = getPreCvData(date, 3, spark)

    val data = preCvrData
      .join(postCvrData, Seq("unitid", "deep_conversion_goal", "media", "date"), "outer")
      .na.fill(0.0, Seq("total_exp_cvr", "total_retention_cvr", "activation_cv", "retention_cv", "click"))
      .withColumn("media", udfMediaName()(col("media")))
      .select("unitid", "deep_conversion_goal", "media", "date", "total_exp_cvr", "total_retention_cvr", "activation_cv", "retention_cv", "click")
      .withColumn("time_window", udfLabelTimeWindow(date)(col("date")))

    data
  }

  def udfLabelTimeWindow(date: String) = udf((dateCol: String) => {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val date1 = dateConverter.format(calendar.getTime)
    calendar.add(Calendar.DATE, -1)
    val date2 = dateConverter.format(calendar.getTime)

    val result = {
      if (dateCol == date) {
        1
      } else if (dateCol == date1) {
        2
      } else if (dateCol == date2) {
        3
      } else {
        0
      }
    }
    result

  })



  def getPreCvData(date: String, dayInt: Int, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayInt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"date between '$date1' and '$date'"

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  deep_conversion_goal,
         |  exp_cvr,
         |  deep_cvr * 1.0 / 1000000 as retention_cvr,
         |  isclick,
         |  media_appsid,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  is_ocpc = 1
         |AND
         |  is_deep_ocpc = 1
         |AND
         |  isclick=1
         |AND
         |  deep_conversion_goal = 2
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .groupBy("unitid", "deep_conversion_goal", "media", "date")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("exp_cvr")).alias("total_exp_cvr"),
        sum(col("retention_cvr")).alias("total_retention_cvr")
      )
      .select("unitid", "deep_conversion_goal", "media", "date", "click", "total_exp_cvr", "total_retention_cvr")

    data
  }

  def getPostCvData(date: String, dayInt: Int, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -dayInt)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"day between '$date1' and '$date'"

    // 激活数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  media_appsid,
         |  1 as iscvr1,
         |  day as date
         |FROM
         |  dl_cpc.cpc_conversion
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
         |AND
         |  array_contains(conversion_target, 'api_app_active')
         |""".stripMargin
    println(sqlRequest1)
    val data1 = spark
      .sql(sqlRequest1)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .distinct()

    // 次留数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr2
         |FROM
         |  dl_cpc.cpc_conversion
         |WHERE
         |  day >= '$date1'
         |AND
         |  $mediaSelection
         |AND
         |  array_contains(conversion_target, 'api_app_retention')
         |""".stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .distinct()

    val data = data1
      .join(data2, Seq("searchid"), "left_outer")
      .groupBy("unitid", "media", "date")
      .agg(
        sum(col("iscvr1")).alias("activation_cv"),
        sum(col("iscvr2")).alias("retention_cv")
      )
      .withColumn("deep_conversion_goal", lit(2))
      .select("unitid", "deep_conversion_goal", "media", "date", "activation_cv", "retention_cv")

    data
  }




}
