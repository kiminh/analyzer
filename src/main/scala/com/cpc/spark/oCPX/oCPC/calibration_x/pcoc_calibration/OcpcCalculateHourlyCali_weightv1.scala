package com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_calibration

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcCalculateHourlyCali_weightv1{
  /*
  calculate the pcoc incrementation by hour
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = 89

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, expTag=$expTag")

    // base data
    val dataRaw = OcpcCalibrationBase(date, hour, hourInt, spark).cache()
    dataRaw.show(10)

    // cvr_factor based on previous pcoc
    val previousPcoc = OcpcPrevPcoc(dataRaw, expTag, spark)

    // current pcoc
    val currentPcoc = OcpcCurrentPcoc(dataRaw, expTag, spark)

    // data join
    val data = currentPcoc
      .join(previousPcoc, Seq("unitid", "conversion_goal", "exp_tag"), "inner")
      .select("unitid", "conversion_goal", "exp_tag", "pcoc", "current_pcoc")

    val resultDF = data
      .select("unitid", "conversion_goal", "pcoc", "current_pcoc")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .withColumn("exp_tag", col("exp_tag"))


    resultDF
      .write.mode("overwrite").saveAsTable("test.check_ocpc_base_data20191225")

  }


  def assemblyData(jfbData: DataFrame, pcocData: DataFrame, spark: SparkSession) = {
    // 组装数据
    val data = jfbData
      .join(pcocData, Seq("unitid", "conversion_goal", "exp_tag"), "inner")
      .select("unitid", "conversion_goal", "exp_tag", "jfb_factor", "cvr_factor")
      .withColumn("high_bid_factor", lit(1.0))
      .withColumn("low_bid_factor", lit(1.0))
      .withColumn("post_cvr", lit(0.0))
      .withColumn("smooth_factor", lit(0.0))
      .na.fill(1.0, Seq("jfb_factor", "cvr_factor"))

    data
  }

  /*
  基础数据
   */
  def OcpcCalibrationBase(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    /*

     */
    val baseDataRaw = getBaseData(hourInt, date, hour, spark)
    val baseData = baseDataRaw
      .withColumn("bid", udfCalculateBidWithHiddenTax()(col("date"), col("bid"), col("hidden_tax")))
      .withColumn("price", udfCalculatePriceWithHiddenTax()(col("price"), col("hidden_tax")))
      .withColumn("hour_diff", udfCalculateHourDiff(date, hour)(col("date"), col("hour")))

    // 计算结果
    val resultDF = calculateParameter(baseData, spark)

    resultDF
  }

  def udfCalculateHourDiff(date: String, hour: String) = udf((date1: String, hour1: String) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")

    val nowTime = dateConverter.parse(date + " " + hour)
    val ocpcTime = dateConverter.parse(date1 + " " + hour1)
    val hourDiff = (nowTime.getTime() - ocpcTime.getTime()) / (1000 * 60 * 60)

    hourDiff
  })

  def calculateParameter(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("unitid", "conversion_goal", "media", "date", "hour", "hour_diff")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        avg(col("bid")).alias("acb"),
        avg(col("price")).alias("acp"),
        avg(col("exp_cvr")).alias("pre_cvr")
      )
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("unitid", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc", "acb", "acp", "date", "hour", "hour_diff")

    data
  }

  def getDataByTimeSpan(dataRaw: DataFrame, date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    dataRaw.createOrReplaceTempView("raw_data")

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
         |    unitid,
         |    conversion_goal,
         |    media,
         |    sum(click) as click,
         |    sum(cv) as cv,
         |    sum(pre_cvr * click) * 1.0 / sum(click) as pre_cvr,
         |    sum(cv) * 1.0 / sum(click) as post_cvr,
         |    sum(acb * click) * 1.0 / sum(click) as acb,
         |    sum(acp * click) * 1.0 / sum(click) as acp
         |FROM
         |    raw_data
         |WHERE
         |    $selectCondition
         |GROUP BY unitid, conversion_goal, media
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("unitid", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc", "acb", "acp")

    data
  }

  def getDataByHourDiff(dataRaw: DataFrame, leftHourBound: Int, rightHourBound: Int, spark: SparkSession) = {
    dataRaw
      .createOrReplaceTempView("raw_data")

    val selectCondition = s"hour_diff >= $leftHourBound and hour_diff < $rightHourBound"
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    conversion_goal,
         |    media,
         |    sum(click) as click,
         |    sum(cv) as cv,
         |    sum(pre_cvr * click) * 1.0 / sum(click) as pre_cvr,
         |    sum(cv) * 1.0 / sum(click) as post_cvr,
         |    sum(acb * click) * 1.0 / sum(click) as acb,
         |    sum(acp * click) * 1.0 / sum(click) as acp
         |FROM
         |    raw_data
         |WHERE
         |    $selectCondition
         |GROUP BY unitid, conversion_goal, media
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("unitid", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc")

    data
  }

  /*
  current pcoc
   */
  def OcpcCurrentPcoc(dataRaw: DataFrame, expTag: String, spark: SparkSession) = {
    /*
    calculate the calibration value based on weighted calibration:
    case: hourDiff = 0

    use 80 as cv threshold
    if the cv < min_cv, rollback to the upper layer(case1 -> case2, etc.)
     */
    val data = getDataByHourDiff(dataRaw, 0, 1, spark)
    val result = data
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("current_pcoc", col("pcoc"))
      .filter(s"cv >= 40")
    result.show(10)

    val resultDF = result
      .select("unitid", "conversion_goal", "exp_tag", "current_pcoc")

    resultDF

  }

  /*
  previous pcoc prediction
   */
  def OcpcPrevPcoc(dataRaw: DataFrame, expTag: String, spark: SparkSession) = {
    /*
    calculate the calibration value based on weighted calibration:
    case1: 0 ~ 5: 0.4
    case2: 0 ~ 12: 0.3
    case3: 0 ~ 24: 0.2
    case4: 0 ~ 48: 0.05
    case5: 0 ~ 84: 0.05

    use 80 as cv threshold
    if the cv < min_cv, rollback to the upper layer(case1 -> case2, etc.)
     */
    val dataRaw1 = getDataByHourDiff(dataRaw, 5, 11, spark)
    val data1 = dataRaw1
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .filter(s"cv >= 80")
    data1.show(10)

    val dataRaw2 = getDataByHourDiff(dataRaw, 5, 17, spark)
    val data2 = dataRaw2
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .filter(s"cv >= 80")
    data2.show(10)

    val dataRaw3 = getDataByHourDiff(dataRaw, 5, 29, spark)
    val data3 = dataRaw3
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .filter(s"cv >= 80")
    data3.show(10)

    val dataRaw4 = getDataByHourDiff(dataRaw, 5, 53, spark)
    val data4 = dataRaw4
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .filter(s"cv >= 80")
    data4.show(10)

    val dataRaw5 = getDataByHourDiff(dataRaw, 5, 89, spark)
    val data5 = dataRaw5
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .filter(s"cv > 0")
    data5.show(10)


    // 计算最终值
    val calibration = calculateCalibrationValuePCOC(data1, data2, data3, data4, data5, spark)

    val resultDF = calibration
      .select("unitid", "conversion_goal", "exp_tag", "pcoc")

    resultDF

  }

  def calculateCalibrationValuePCOC(dataRaw1: DataFrame, dataRaw2: DataFrame, dataRaw3: DataFrame, dataRaw4: DataFrame, dataRaw5: DataFrame, spark: SparkSession) = {
    /*
    calculate the calibration value based on weighted calibration:
    case1: 0 ~ 5: 0.4
    case2: 0 ~ 12: 0.3
    case3: 0 ~ 24: 0.2
    case4: 0 ~ 48: 0.05
    case5: 0 ~ 84: 0.05

    pcoc = 0.4 * pcoc1 + 0.3 * pcoc2 + 0.2 * pcoc3 + 0.05 * pcoc4 + 0.05 * pcoc5
     */
    // case1
    val data1 = dataRaw1
      .withColumn("pcoc1", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "pcoc1")

    // case2
    val data2 = dataRaw2
      .withColumn("pcoc2", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "pcoc2")

    // case3
    val data3 = dataRaw3
      .withColumn("pcoc3", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "pcoc3")

    // case4
    val data4 = dataRaw4
      .withColumn("pcoc4", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "pcoc4")

    // case5
    val data5 = dataRaw5
      .withColumn("pcoc5", col("pcoc"))
      .select("unitid", "conversion_goal", "exp_tag", "pcoc5")


    val baseData = data5
        .join(data4, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
        .join(data3, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
        .join(data2, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
        .join(data1, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
        .withColumn("pcoc4_old", col("pcoc4"))
        .withColumn("pcoc3_old", col("pcoc3"))
        .withColumn("pcoc2_old", col("pcoc2"))
        .withColumn("pcoc1_old", col("pcoc1"))
        .withColumn("pcoc4", when(col("pcoc4").isNull, col("pcoc5")).otherwise(col("pcoc4")))
        .withColumn("pcoc3", when(col("pcoc3").isNull, col("pcoc4")).otherwise(col("pcoc3")))
        .withColumn("pcoc2", when(col("pcoc2").isNull, col("pcoc3")).otherwise(col("pcoc2")))
        .withColumn("pcoc1", when(col("pcoc1").isNull, col("pcoc2")).otherwise(col("pcoc1")))

    baseData
        .write.mode("overwrite").saveAsTable("test.ocpc_check_data20191225a")
    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  conversion_goal,
         |  exp_tag,
         |  pcoc1,
         |  pcoc2,
         |  pcoc3,
         |  pcoc4,
         |  pcoc5,
         |  (0.4 * pcoc1 + 0.3 * pcoc2 + 0.2 * pcoc3 + 0.05 * pcoc4 + 0.05 * pcoc5) as pcoc
         |FROM
         |  base_data
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val resultDF = data.cache()

    resultDF.show()

    resultDF

  }
}


