package com.cpc.spark.oCPX.deepOcpc.calibration_tools

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcCalibrationBase._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcJFBfactor {
  def main(args: Array[String]): Unit = {
    /*
    计算计费比
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString

    // 主校准回溯时间长度
    val hourInt1 = args(4).toInt
    // 备用校准回溯时间长度
    val hourInt2 = args(5).toInt
    // 兜底校准时长
    val hourInt3 = args(6).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, expTag=$expTag, hourInt1=$hourInt1, hourInt2=$hourInt2, hourInt3=$hourInt3")

    val dataRaw = OcpcCalibrationBaseDelayMain(date, hour, hourInt3, spark).cache()

    val result = OcpcJFBfactorMain(date, hour, version, expTag, dataRaw, hourInt1, hourInt2, hourInt3, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_jfb_factor20190723b")
  }

  def OcpcJFBfactorMain(date: String, hour: String, version: String, expTag: String, dataRaw: DataFrame, hourInt1: Int, hourInt2: Int, hourInt3: Int, spark: SparkSession) = {
    val minCV = 0

    val data1 = selectDataByHourInt(dataRaw, date, hour, hourInt1, expTag, minCV, spark)

    val data2 = selectDataByHourInt(dataRaw, date, hour, hourInt2, expTag, minCV, spark)

    val data3 = selectDataByHourInt(dataRaw, date, hour, hourInt3, expTag, minCV, spark)

    // 计算最终值
    val calibration1 = calculateCalibrationValue(data1, data2, spark)
    val calibrationNew = data3
      .filter(s"cv >= min_cv")
      .withColumn("jfb_new", col("jfb"))
      .select("identifier", "conversion_goal", "exp_tag", "jfb_new")

    val calibration = calibrationNew
      .join(calibration1, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .withColumn("jfb", when(col("jfb3").isNotNull, col("jfb3")).otherwise(col("jfb_new")))
      .cache()

    calibration.show(10)
    calibration
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_jfb_factor20190723a")

    val resultDF = calibration
      .withColumn("version", lit(version))
      .select("identifier", "conversion_goal", "exp_tag", "version", "jfb")


    resultDF
  }

  def selectDataByHourInt(dataRaw: DataFrame, date: String, hour: String, hourInt: Int, expTag: String, minCV: Int, spark: SparkSession) = {
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

    val data = dataRaw
      .filter(selectCondition)
      .groupBy("identifier", "conversion_goal", "media")
      .agg(
        sum(col("click")).alias("click"),
        sum(col("cv")).alias("cv"),
        sum(col("total_bid")).alias("total_bid"),
        sum(col("total_price")).alias("total_price")
      )
      .select("identifier", "conversion_goal", "media", "click", "cv", "total_bid", "total_price")
      .na.fill(0, Seq("cv"))
      .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("min_cv", lit(minCV))
      .filter(s"cv > 0")
    data.show(10)

    data
  }

  def calculateCalibrationValue(dataRaw1: DataFrame, dataRaw2: DataFrame, spark: SparkSession) = {
    /*
    "identifier", "click", "cv", "pre_cvr", "total_price", "total_bid"
     */

    // 主校准模型
    val data1 = dataRaw1
      .filter(s"cv >= min_cv")
      .withColumn("jfb1", col("jfb"))
      .select("identifier", "conversion_goal", "exp_tag", "jfb1")

    // 备用校准模型
    val data2 = dataRaw2
      .filter(s"cv >= min_cv")
      .withColumn("jfb2", col("jfb"))
      .select("identifier", "conversion_goal", "exp_tag", "jfb2")

    // 数据表关联
    val data = data2
      .join(data1, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .na.fill(0, Seq("flag"))
      .withColumn("jfb3", when(col("jfb1").isNotNull, col("jfb1")).otherwise(col("jfb2")))

    data.show()

    data

  }
}