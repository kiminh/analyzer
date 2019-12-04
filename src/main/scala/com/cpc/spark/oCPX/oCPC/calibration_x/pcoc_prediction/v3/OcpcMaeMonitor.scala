package com.cpc.spark.oCPX.oCPC.calibration_x.pcoc_prediction.v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getBaseDataDelay, getTimeRangeSqlDate, udfMediaName, udfSetExpTag}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcMaeMonitor {
  def main(args: Array[String]): Unit = {
    /*
    采用点击加权mae评估前12小时的baseline和pcoc预估校准策略
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag1 = args(3).toString
    val expTag2 = args(4).toString
    val hourDiff = args(5).toInt


    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, expTag1=$expTag1, expTag2=$expTag2, hourDiff=$hourDiff")

    // baseline校准策略的pcoc
    val baselineData = getBaselinePcoc(date, hour, 12, hourDiff, version, expTag2, spark)

    // pcoc预估校准策略的pcoc
    val predData = getPredPcoc(date, hour, 12, hourDiff, version, expTag1, spark)

    // 真实pcoc
    val hourlyPcoc = getRealPcoc(date, hour, 12, spark)

    // 计算分小时分单元分媒体的pcoc差异
    val hourlyDiff = calculateHourlyDiff(hourlyPcoc, baselineData, predData, hourDiff, expTag2, spark).cache()
    hourlyDiff
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("unitid", "time", "click", "cv", "real_pcoc", "baseline_pcoc", "pred_pcoc", "baseline_diff", "pred_diff", "date", "hour", "version", "exp_tag")
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_calibration_method_cmp_hourly")
//      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191204d")

    // 计算点击加权分单元分媒体mae
    val result = calculateMae(hourlyDiff, spark)

    // 生成单元校准策略优先级词表
    val resultDF = generatePriorityTable(result, spark)

    resultDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("unitid", "method", "baseline_mae", "pred_mae", "click", "cv", "date", "hour", "version", "exp_tag")
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_calibration_method_hourly")
  }

  def generatePriorityTable(baseData: DataFrame, spark: SparkSession) = {
    val data1 = baseData
      .filter(s"baseline_mae < pred_mae")
      .withColumn("method", lit("baseline"))
      .select("unitid", "exp_tag", "method", "baseline_mae", "pred_mae", "click", "cv")
      .distinct()
    val data2 = baseData
      .filter(s"baseline_mae >= pred_mae")
      .withColumn("method", lit("pred"))
      .select("unitid", "exp_tag", "method", "baseline_mae", "pred_mae", "click", "cv")
      .distinct()

    val data = data1.union(data2).distinct()

    data
  }

  def calculateMae(baseData: DataFrame, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  exp_tag,
         |  sum(click) as click,
         |  sum(cv) as cv,
         |  sum(baseline_diff * click) * 1.0 / sum(click) as basline_mae,
         |  sum(pred_diff * click) * 1.0 / sum(click) as pred_mae
         |FROM
         |  base_data
         |GROUP BY unitid, exp_tag
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

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


  def calculateHourlyDiff(dataRaw1: DataFrame, dataRaw2: DataFrame, dataRaw3: DataFrame, hourDiff: Int, expTag: String, spark: SparkSession) = {
    val data1 = dataRaw1
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("real_pcoc", col("pcoc"))
      .withColumn("time", concat_ws(" ", col("date"), col("hour")))
      .select("unitid", "exp_tag", "time", "click", "cv", "real_pcoc")

    val data2 = dataRaw2
      .withColumn("baseline_pcoc", col("pcoc"))
      .withColumn("time", udfAddHour(hourDiff)(col("date"), col("hour")))
      .select("unitid", "exp_tag", "time", "baseline_pcoc")

    val data3 = dataRaw3
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .withColumn("pred_pcoc", col("pcoc"))
      .withColumn("time", udfAddHour(hourDiff)(col("date"), col("hour")))
      .select("unitid", "exp_tag", "time", "pred_pcoc")

    val data = data1
      .join(data2, Seq("unitid", "exp_tag", "time"), "inner")
      .join(data3, Seq("unitid", "exp_tag", "time"), "inner")
      .select("unitid", "exp_tag", "time", "click", "cv", "real_pcoc", "baseline_pcoc", "pred_pcoc")

    data.createOrReplaceTempView("data")
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  exp_tag,
         |  time,
         |  click,
         |  cv,
         |  real_pcoc,
         |  baseline_pcoc,
         |  pred_pcoc,
         |  abs(real_pcoc - baseline_pcoc) as baseline_diff,
         |  abs(real_pcoc - pred_pcoc) as pred_diff
         |FROM
         |  data
         |""".stripMargin
    println(sqlRequest)
    val result = spark.sql(sqlRequest)

    result
  }

  def getRealPcoc(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    val baseDataRaw = getBaseDataDelay(hourInt, date, hour, spark)
    baseDataRaw.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  media,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(isclick) as click,
         |  sum(iscvr) as cv,
         |  date,
         |  hour
         |FROM
         |  base_data
         |WHERE
         |  isclick=1
         |GROUP BY unitid, media, date, hour
         |""".stripMargin
    val data = spark
      .sql(sqlRequest)
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("media", udfMediaName()(col("media")))
      .filter(s"pcoc is not null")

    data
  }

  def getPredPcoc(date: String, hour: String, hourInt: Int, hourDiff: Int, version: String, expTag: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    calendar.add(Calendar.HOUR, -hourDiff)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |   cast(identifier as int) as unitid,
         |   media,
         |   pred_pcoc as pcoc,
         |   date,
         |   hour
         |FROM
         |    dl_cpc.ocpc_pcoc_prediction_result_hourly
         |WHERE
         |    $selectCondition
         |AND
         |    version = '$version'
         |AND
         |    exp_tag = '$expTag'
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("media", udfMediaName()(col("media")))
//      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media_new")))

    data
  }

  def getBaselinePcoc(date: String, hour: String, hourInt: Int, hourDiff: Int, version: String, expTag: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    calendar.add(Calendar.HOUR, -hourDiff)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |   unitid,
         |   exp_tag,
         |   1.0 / cvr_factor as pcoc,
         |   date,
         |   hour
         |FROM
         |    dl_cpc.ocpc_pb_data_hourly_exp
         |WHERE
         |    $selectCondition
         |AND
         |    version = '$version'
         |AND
         |    exp_tag like '${expTag}%'
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }



}


