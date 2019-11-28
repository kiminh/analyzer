package com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction.v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfConcatStringInt, udfDetermineMedia}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object prepareSample {
  def main(args: Array[String]): Unit = {
    /*
    采用拟合模型进行pcoc的时序预估
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt
    val version = args(3).toString
    val expTag = args(4).toString
    val minCV = args(5).toInt


    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt=$hourInt, version=$version, expTag=$expTag")

    val rawData = getBaseData(date, hour, hourInt, spark)

    val baseData = calculateBaseData(rawData, spark).cache()
    baseData.show(10)

    val result = assemblyData(baseData, date, hour, minCV, spark)

    result
      .repartition(1)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .withColumn("exp_tag", lit(expTag))
      .write.mode("overwrite").insertInto("test.ocpc_pcoc_sample_part_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pcoc_sample_part1_hourly")
  }

  def assemblyData(dataRaw: DataFrame, date: String, hour: String, minCV: Int, spark: SparkSession) = {
    val data1 = calculateSummaryPCOC(dataRaw, date, hour, 6, minCV, "6", spark)
    val data2 = calculateSummaryPCOC(dataRaw, date, hour, 12, minCV, "12", spark)
    val data3 = calculateSummaryPCOC(dataRaw, date, hour, 24, minCV, "24", spark)
    val data4 = calculateSummaryPCOC(dataRaw, date, hour, 48, minCV, "48", spark)
    val data5 = calculateSummaryPCOC(dataRaw, date, hour, 72, minCV, "72", spark)

    val result = data1
      .join(data2, Seq("identifier", "media", "conversion_goal", "conversion_from"), "inner")
      .join(data3, Seq("identifier", "media", "conversion_goal", "conversion_from"), "inner")
      .join(data4, Seq("identifier", "media", "conversion_goal", "conversion_from"), "inner")
      .join(data5, Seq("identifier", "media", "conversion_goal", "conversion_from"), "inner")
      .selectExpr("identifier", "media", "conversion_goal", "conversion_from", "pcoc6", "pcoc12", "pcoc24", "pcoc48", "pcoc72", "cv6", "cv12", "cv24", "cv48", "cv72")
      .withColumn("double_feature_list", udfDoubleFeatures()(col("pcoc6"), col("pcoc12"), col("pcoc24"), col("pcoc48"), col("pcoc72"), col("cv6"), col("cv12"), col("cv24"), col("cv48"), col("cv72")))
      .withColumn("string_feature_list", udfStringFeatures()(col("cv6"), col("cv12"), col("cv24"), col("cv48"), col("cv72")))
      .select("identifier", "media", "conversion_goal", "conversion_from", "double_feature_list", "string_feature_list")

    result
  }

  def udfAggregateFeature() = udf((value1: Double, value2: Double, value3: Double, value4: Double, value5: Double, value6: Double, value7: Double, value8: Double, value9: Double, value10: Double) => {
    val result = Array(value1, value2, value3, value4, value5, value6, value7, value8, value9, value10)
    result
  })

  def udfDoubleFeatures() = udf((value1: Double, value2: Double, value3: Double, value4: Double, value5: Double) => {
    val result = Array(value1, value2, value3, value4, value5)
    result
  })

  def udfStringFeatures() = udf((value1: String, value2: String, value3: String, value4: String, value5: String) => {
    val result = Array(value1, value2, value3, value4, value5)
    result
  })



  def getBaseData(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
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
         |  cast(unitid as string) as identifier,
         |  unitid,
         |  userid,
         |  adslot_type,
         |  isshow,
         |  isclick,
         |  cast(exp_cvr as double) as exp_cvr,
         |  media_appsid,
         |  (case
         |      when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |      when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |      when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |      when adclass in (110110100, 125100100) then "wzcp"
         |      else "others"
         |  end) as industry,
         |  conversion_goal,
         |  conversion_from,
         |  cast(ocpc_log_dict['cvr_factor'] as double) as cvr_factor,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_filter_unionlog
         |WHERE
         |  $selectCondition
         |AND
         |  is_ocpc = 1
         |AND
         |  isclick = 1
       """.stripMargin
    println(sqlRequest)
    val clickData = spark
      .sql(sqlRequest)
      .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
      .withColumn("media", udfDetermineMedia()(col("media_appsid")))

    // 抽取cv数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvData = spark.sql(sqlRequest2).distinct()

    // 数据关联
    val resultDF = clickData
      .join(cvData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

    resultDF
  }

  def calculateBaseData(dataRaw: DataFrame, spark: SparkSession) = {
    // 数据关联
    val resultDF = dataRaw
      .groupBy("identifier", "media", "conversion_goal", "conversion_from", "date", "hour")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        sum(col("exp_cvr")).alias("total_pre_cvr")
      )
      .select("identifier", "media", "conversion_goal", "conversion_from", "date", "hour", "click", "cv", "total_pre_cvr")

    resultDF

  }

  def calculateSummaryPCOC(dataRaw: DataFrame, date: String, hour: String, hourInt: Int, minCV: Int, outputCol: String, spark: SparkSession) = {
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
         |  identifier,
         |  media,
         |  conversion_goal,
         |  conversion_from,
         |  sum(cv) * 1.0 / sum(click) as post_cvr,
         |  sum(total_pre_cvr) * 1.0 / sum(click) as pre_cvr,
         |  sum(cv) as cv
         |FROM
         |  raw_data
         |WHERE
         |  $selectCondition
         |GROUP BY identifier, media, conversion_goal, conversion_from
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("pcoc" + outputCol, col("pre_cvr") * 1.0 / col("post_cvr"))
      .filter(s"cv > 0")
      .withColumn("cv" + outputCol, when(col("cv") > minCV, "1").otherwise("0"))
      .select("identifier", "media", "conversion_goal", "conversion_from", "pcoc" + outputCol, "cv" + outputCol)

    data
  }

}


