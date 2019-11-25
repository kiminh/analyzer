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
    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt
    val version = args(3).toString
    val expTag = args(4).toString
    val minCV = args(5).toInt


    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt=$hourInt, version=$version, expTag=$expTag")

    val rawData = getBaseData(date, hour, hourInt, spark).cache()
    val baseData = calculateBaseData(rawData, spark).cache()

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

    val recentData = getRecentPcoc(baseData, date, hour, spark)

    val result = assemblyData(avgPcoc, diffPcoc1, diff2Pcoc, recentData, minCV, spark)

    result
      .repartition(1)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .withColumn("exp_tag", lit(expTag))
      .write.mode("overwrite").insertInto("test.ocpc_pcoc_sample_part1_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pcoc_sample_part1_hourly")
  }

  def assemblyData(dataRaw1: DataFrame, dataRaw2: DataFrame, dataRaw3: DataFrame, dataRaw4: DataFrame, minCV: Int, spark: SparkSession) = {
    val data1 = dataRaw1
      .withColumn("avg_pcoc", col("pcoc"))
      .select("identifier", "media", "conversion_goal", "conversion_from", "avg_pcoc")
      .filter(s"avg_pcoc is not null")

    val data2 = dataRaw2
      .withColumn("diff1_pcoc", col("value"))
      .select("identifier", "media", "conversion_goal", "conversion_from", "diff1_pcoc")
      .filter(s"diff1_pcoc is not null")

    val data3 = dataRaw3
      .withColumn("diff2_pcoc", col("value"))
      .select("identifier", "media", "conversion_goal", "conversion_from", "diff2_pcoc")
      .filter(s"diff2_pcoc is not null")

    val data4 = dataRaw4
      .filter(s"recent_pcoc is not null and recent_cv >= $minCV")
      .select("identifier", "media", "conversion_goal", "conversion_from", "recent_pcoc")

    val result = data1
      .join(data2, Seq("identifier", "media", "conversion_goal", "conversion_from"), "inner")
      .join(data3, Seq("identifier", "media", "conversion_goal", "conversion_from"), "inner")
      .join(data4, Seq("identifier", "media", "conversion_goal", "conversion_from"), "inner")
      .selectExpr("identifier", "media", "conversion_goal", "conversion_from", "cast(avg_pcoc as double) avg_pcoc", "cast(diff1_pcoc as double) as diff1_pcoc", "cast(diff2_pcoc as double) as diff2_pcoc", "cast(recent_pcoc as double) as recent_pcoc")
      .withColumn("feature_list", udfAggregateFeature()(col("avg_pcoc"), col("diff1_pcoc"), col("diff2_pcoc"), col("recent_pcoc")))
      .select("identifier", "media", "conversion_goal", "conversion_from", "feature_list")

    result
  }

  def udfAggregateFeature() = udf((avgPcoc: Double, diff1Pcoc: Double, diff2Pcoc: Double, recentPcoc: Double) => {
    val result = Array(avgPcoc, diff1Pcoc, diff2Pcoc, recentPcoc)
    result
  })

  def getRecentPcoc(dataRaw: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val result = dataRaw
      .filter(s"`date` = '$date' and `hour` = '$hour'")
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("recent_pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("identifier", "media", "conversion_goal", "conversion_from", "recent_pcoc", "recent_cv")

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
      .select("identifier", "media", "conversion_goal", "conversion_from", "value")

    val dataRaw1 = dataRaw
      .filter(s"`date` = '$date1' and `hour` = '$hour1'")
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("value", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("identifier", "media", "conversion_goal", "conversion_from", "value")

    val diffData = calculateDiffData(dataRaw0, dataRaw1, spark)

    diffData
  }

  def calculateDiffData(dataRaw1: DataFrame, dataRaw2: DataFrame, spark: SparkSession) = {
    val data1 = dataRaw1
      .withColumn("value1", col("value"))
      .select("identifier", "media", "conversion_goal", "conversion_from", "value1")

    val data2 = dataRaw2
      .withColumn("value2", col("value"))
      .select("identifier", "media", "conversion_goal", "conversion_from", "value2")

    val data = data1
      .join(data2, Seq("identifier", "media", "conversion_goal", "conversion_from"), "inner")
      .withColumn("value", col("value1") - col("value2"))
      .select("identifier", "media", "conversion_goal", "conversion_from", "value")

    data
  }

  def getBasePcoc(dataRaw: DataFrame, spark: SparkSession) = {
    val result = dataRaw
      .groupBy("identifier", "media", "conversion_goal", "conversion_from")
      .agg(
        sum(col("click")).alias("click"),
        sum(col("cv")).alias("cv"),
        sum(col("total_pre_cvr")).alias("total_pre_cvr")
      )
      .select("identifier", "media", "conversion_goal", "conversion_from", "click", "cv", "total_pre_cvr")
      .withColumn("pre_cvr", col("total_pre_cvr") * 1.0 / col("click"))
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("identifier", "media", "conversion_goal", "conversion_from", "pcoc")

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

}


