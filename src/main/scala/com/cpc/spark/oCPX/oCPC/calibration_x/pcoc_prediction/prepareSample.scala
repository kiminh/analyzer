package com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfConcatStringInt, udfDetermineMedia}
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.{OneHotEncoder, StandardScaler, StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object prepareSample {
  def main(args: Array[String]): Unit = {
    /*
    采用拟合模型进行pcoc的时序预估
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // 计算日期周期
    // bash: 2019-01-02 12 1 qtt_demo qtt
    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt


    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt=$hourInt")

    val rawData = getBaseData(date, hour, hourInt, spark).cache()
    val baseData = calculateBaseData(rawData, spark)
    val avgPcoc = getBasePcoc(baseData, spark)
    val diffPcoc1 = getDiffPcoc(baseData, date, hour, spark)

    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -1)
    val tmpDate1 = dateConverter.format(calendar.getTime)
    val tmpDateValue1 = tmpDate1.split(" ")
    val date1 = tmpDateValue1(0)
    val hour1 = tmpDateValue1(1)

    val diffPcoc2 = getDiffPcoc(baseData, date1, hour1, spark)

    val diff2Pcoc = calculateDiffData(diffPcoc1, diffPcoc2, spark)

    val result = assemblyData(avgPcoc, diffPcoc1, diff2Pcoc, spark)

    result
      .write.mode("overwrite").saveAsTable("test.check_ocpc_result_data20191122a")
  }

  def assemblyData(dataRaw1: DataFrame, dataRaw2: DataFrame, dataRaw3: DataFrame, spark: SparkSession) = {
    val data1 = dataRaw1
      .withColumn("avg_pcoc", col("pcoc"))
      .select("identifier", "media", "conversion_goal", "avg_pcoc")

    val data2 = dataRaw2
      .withColumn("diff1_pcoc", col("value"))
      .select("identifier", "media", "conversion_goal", "diff1_pcoc")

    val data3 = dataRaw3
      .withColumn("diff2_pcoc", col("value"))
      .select("identifier", "media", "conversion_goal", "diff2_pcoc")

    val result = data1
      .join(data2, Seq("identifier", "media", "conversion_goal"), "inner")
      .join(data3, Seq("identifier", "media", "conversion_goal"), "inner")
      .select("identifier", "media", "conversion_goal", "avg_pcoc", "diff1_pcoc", "diff2_pcoc")

    result
  }

  def getDiffPcoc(dataRaw: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -1)
    val tmpDate1 = dateConverter.format(calendar.getTime)
    val tmpDateValue1 = tmpDate1.split(" ")
    val date1 = tmpDateValue1(0)
    val hour1 = tmpDateValue1(1)

    val dataRaw0 = dataRaw
      .filter(s"`date` = '$date' and `hour` = '$hour'")
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("value", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("identifier", "media", "conversion_goal", "value")

    val dataRaw1 = dataRaw
      .filter(s"`date` = '$date1' and `hour` = '$hour1'")
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("value", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("identifier", "media", "conversion_goal", "value")

    val diffData = calculateDiffData(dataRaw0, dataRaw1, spark)

    diffData
  }

  def calculateDiffData(dataRaw1: DataFrame, dataRaw2: DataFrame, spark: SparkSession) = {
    val data1 = dataRaw1
      .withColumn("value1", col("value"))
      .select("identifier", "media", "conversion_goal", "value1")

    val data2 = dataRaw2
      .withColumn("value2", col("value"))
      .select("identifier", "media", "conversion_goal", "value2")

    val data = data1
      .join(data2, Seq("identifier", "media", "conversion_goal"), "inner")
      .withColumn("value", col("value1") - col("value2"))
      .select("identifier", "media", "conversion_goal", "value")

    data
  }

  def getBasePcoc(dataRaw: DataFrame, spark: SparkSession) = {
    val result = dataRaw
      .groupBy("identifier", "media", "conversion_goal")
      .agg(
        sum(col("click")).alias("click"),
        sum(col("cv")).alias("cv"),
        sum(col("total_pre_cvr")).alias("total_pre_cvr")
      )
      .select("identifier", "media", "conversion_goal", "click", "cv", "total_pre_cvr")
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("identifier", "media", "conversion_goal", "pcoc")

    result
  }

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
      .groupBy("identifier", "media", "conversion_goal", "date", "hour")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        sum(col("exp_cvr")).alias("total_pre_cvr")
      )
      .select("identifier", "media", "conversion_goal", "date", "hour", "click", "cv", "total_pre_cvr")

    resultDF

  }

}


