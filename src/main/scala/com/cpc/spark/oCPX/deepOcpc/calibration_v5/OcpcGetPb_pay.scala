package com.cpc.spark.oCPX.deepOcpc.calibration_v5

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfCalculateBidWithHiddenTax, udfCalculatePriceWithHiddenTax, udfDetermineMedia, udfMediaName, udfSetExpTag}
import com.cpc.spark.oCPX.deepOcpc.calibration_v5.pay.OcpcDeepBase_payfactor.OcpcDeepBase_payfactorMain
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
    val minCV = args(4).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag")

    // 校准系数
    val data1 = OcpcCvrFactor(date, hour, minCV, spark)
    data1
      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191120a")

    // 计费比系数
    val data2 = OcpcJFBfactor(date, hour, spark)
    data2
      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191120b")


    // 计算cvr校准系数
    val data = data1
      .join(data2, Seq("unitid", "deep_conversion_goal", "media"), "inner")
      .withColumn("conversion_goal", col("deep_conversion_goal"))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
    data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191120c")


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


  /*
  校准系数
   */
  def OcpcCvrFactor(date: String, hour: String, minCV: Int, spark: SparkSession) = {
    val data = OcpcDeepBase_payfactorMain(date, hour, minCV, spark)
    val result = data
      .select("unitid", "deep_conversion_goal", "media", "recall_cvr_factor")
      .withColumn("cvr_factor", col("recall_cvr_factor"))
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
         |  hidden_tax,
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
         |  deep_conversion_goal = 3
       """.stripMargin
    println(sqlRequest)
    val rawData = spark
      .sql(sqlRequest)
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("bid", udfCalculateBidWithHiddenTax()(col("date"), col("bid"), col("hidden_tax")))
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))

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
