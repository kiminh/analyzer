package com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, udfConcatStringInt, udfDetermineMedia}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object prepareTrainingSample {
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
    val version = args(3).toString


    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt=$hourInt, version=$version")

    val data1 = getData(date, hour, hourInt, version, "test.ocpc_pcoc_sample_part1_hourly", spark)
    val data2 = getData(date, hour, hourInt, version, "test.ocpc_pcoc_sample_part2_hourly", spark)

    val samples = assemblySample(data1, data2, spark)
    samples
      .select("identifier", "media", "conversion_goal", "conversion_from", "double_feature_list", "string_feature_list", "hour_diff", "time", "label")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .write.mode("overwrite").insertInto("test.ocpc_pcoc_sample_hourly")
  }

  def assemblySample(dataRaw1: DataFrame, dataRaw2: DataFrame, spark: SparkSession) = {
    val data1 = dataRaw1
      .withColumn("time", udfAddHour(4)(col("date"), col("hour")))
      .withColumn("hour_diff", lit(4))
      .select("identifier", "media", "conversion_goal", "conversion_from", "feature_list", "time", "date", "hour", "hour_diff")

    val data2 = dataRaw2
      .withColumn("time", concat_ws(" ", col("date"), col("hour")))
      .withColumn("label", col("pcoc"))
      .filter("label is not nulli")
      .select("identifier", "media", "conversion_goal", "conversion_from", "label", "time", "date", "hour")

    val data = data1
      .select("identifier", "media", "conversion_goal", "conversion_from", "feature_list", "time")
      .join(data2, Seq("identifier", "media", "conversion_goal", "conversion_from", "time"), "inner")
      .withColumn("string_feature_list", udfStringFeatures()(col("hour")))
      .withColumn("double_feature_list", col("feature_list"))
      .select("identifier", "media", "conversion_goal", "conversion_from", "double_feature_list", "string_feature_list", "hour", "time", "label", "hour_diff")

    data
  }

  def udfStringFeatures() = udf((hr: String) => {
    val result = Array(hr)
    result
  })

  def udfAddHour(hourInt: Int) = udf((date: String, hour: String) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, hourInt)
    val nextDay = calendar.getTime
    val result = dateConverter.format(nextDay)

    result
  })

  def getData(date: String, hour: String, hourInt: Int, version: String, tableName: String, spark: SparkSession) = {
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
         |  *
         |FROM
         |  $tableName
         |WHERE
         |  $selectCondition
         |AND
         |  version = '$version'
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

}


