package com.cpc.spark.oCPX.deepOcpc.calibration_v2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcRetentionFactor {
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

    val result = OcpcRetentionFactorMain(date, expTag, minCV, spark).cache()

    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_ocpc_factor20191029b")
  }

  def OcpcRetentionFactorMain(date: String, expTag: String, minCV: Int, spark: SparkSession) = {
    val postCvrData = getDeepCvr(date, expTag, spark)
    val preCvrData = getPreCvrData(date, expTag, spark)

    val data = preCvrData
      .join(postCvrData, Seq("unitid", "media"), "inner")
      .withColumn("media", udfMediaName()(col("media")))
      .select("unitid", "media", "pre_cvr1", "pre_cvr2", "cv1", "cv2", "deep_cvr")
      .withColumn("min_cv", lit(minCV))

    data
  }

  def getPreCvrData(date: String, expTag: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -2)
    val date1String = calendar.getTime
    val date1 = dateConverter.format(date1String)

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
         |  media_appsid
         |FROM
         |  dl_cpc.ocpc_base_unionlog
         |WHERE
         |  date = '$date1'
         |AND
         |  is_ocpc = 1
         |AND
         |  is_deep_ocpc = 1
         |AND
         |  isclick=1
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .filter(s"deep_conversion_goal = 2")
      .groupBy("unitid", "media")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("exp_cvr")).alias("total_pre_cvr1"),
        sum(col("retention_cvr")).alias("total_pre_cvr2")
      )
      .withColumn("pre_cvr1", col("total_pre_cvr1") * 1.0 / col("click"))
      .withColumn("pre_cvr2", col("total_pre_cvr2") * 1.0 / col("click"))

    data
  }

  def getDeepCvr(date: String, expTag: String, spark: SparkSession) = {
    // 抽取媒体id
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val date1String = calendar.getTime
    val date1 = dateConverter.format(date1String)
    calendar.add(Calendar.DATE, -1)
    val date2String = calendar.getTime
    val date2 = dateConverter.format(date2String)

    // 激活数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  media_appsid,
         |  1 as iscvr1
         |FROM
         |  dl_cpc.cpc_conversion
         |WHERE
         |  day = '$date2'
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
         |  day = '$date1'
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
      .groupBy("unitid", "media")
      .agg(
        sum(col("iscvr1")).alias("cv1"),
        sum(col("iscvr2")).alias("cv2")
      )
      .select("unitid", "media", "cv1", "cv2")
      .withColumn("deep_cvr", col("cv2") * 1.0 / col("cv1"))

    data
  }

}