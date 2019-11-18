package com.cpc.spark.oCPX.deepOcpc.calibration_v5

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfDetermineMedia, udfMediaName, udfSetExpTag}
import com.cpc.spark.oCPX.deepOcpc.calibration_v5.retention.OcpcDeepBase_deepfactor.OcpcDeepBase_deepfactorMain
import com.cpc.spark.oCPX.deepOcpc.calibration_v5.retention.OcpcDeepBase_shallowfactor.OcpcDeepBase_shallowfactorMain
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcRetentionFactor._
//import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcShallowFactor._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_retention {
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
    val version = args(2).toString
    val expTag = args(3).toString
    val minCV1 = args(4).toInt
    val minCV2 = args(5).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, minCV1=$minCV1, minCV2=$minCV2")

    // 校准系数
    val data1 = OcpcCvrFactor(date, hour, expTag, minCV1, minCV2, spark)
    data1
      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191029a")

    // 计费比系数
    val data2 = OcpcJFBfactor(date, hour, spark)
    data2
      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191029b")


    // 计算cvr校准系数
    val data = data1
      .join(data2, Seq("unitid", "deep_conversion_goal", "media"), "inner")
      .withColumn("conversion_goal", col("deep_conversion_goal"))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))

    data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191029c")


    // 数据组装
    val resultData = assemblyData(data, spark)

    resultData
      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191029d")


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
      .withColumn("deep_conversion_goal", lit(2))
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_deep_pb_data_hourly_exp")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_pb_data_hourly_exp")
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

  def udfSetSmoothFactor() = udf((identifier: String, smoothFactor: Double) => {
    val result = (identifier, smoothFactor) match {
      case ("2399667", _) => 0.7
      case (_, v) => v
    }
    result
  })

  /*
  校准系数
   */
  def OcpcCvrFactor(date: String, hour: String, expTag: String, minCV1: Int, minCV2: Int, spark: SparkSession) = {
    // 激活率因子
    val resultActivation = OcpcDeepBase_shallowfactorMain(date, hour, minCV1, spark)

    // 次留率因子
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -2)
    val date1String = calendar.getTime
    val date1 = dateConverter.format(date1String)
    val resultRetention = OcpcDeepBase_deepfactorMain(date1, minCV2, spark)

    val result = resultActivation
      .join(resultRetention, Seq("unitid", "deep_conversion_goal", "media"), "inner")
      .select("unitid", "deep_conversion_goal", "media", "recall_shallow_factor", "deep_factor")
      .withColumn("cvr_factor", col("recall_shallow_factor") * col("deep_factor"))
      .filter(s"cvr_factor is not null")
      .cache()

    result.show(10)

    result
  }


  /*
  计费比系数
   */
  def OcpcJFBfactor(date: String, hour: String, spark: SparkSession) = {
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
    calendar.add(Calendar.HOUR, -24)
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
         |  conversion_goal,
         |  deep_conversion_goal,
         |  expids,
         |  exptags,
         |  ocpc_expand,
         |  (case when hidden_tax is null then 0 else hidden_tax end) as hidden_tax,
         |  exp_cvr
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
    val rawData = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("price", col("price") - col("hidden_tax"))

    val result = rawData
      .groupBy("unitid", "deep_conversion_goal", "media")
      .agg(
        avg(col("bid")).alias("acb"),
        avg(col("price")).alias("acp")
      )
      .withColumn("jfb_factor", col("acb") * 1.0 / col("acp"))
      .filter("jfb_factor is not null")
      .cache()

    result.show(10)
    result
  }



}
