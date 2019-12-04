package com.cpc.spark.oCPX.oCPC.calibration_by_tag.pred_v1

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag1 = args(3).toString
    val expTag2 = args(4).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag1:$expTag1, expTag2:$expTag2")

    // 读取baseline校准数据
    val baselineData = getBaselineData(date, hour, version, expTag2, spark)

    // 读取pcoc预估模型校准数据
    val predictionData = getPredictionData(date, hour, version, expTag1, spark)

    // 读取筛选词表
    val selectionTable = getSelectionTable(date, hour, version, expTag2, spark)

    // 数据合并
    val data = assemblyData(selectionTable, baselineData, predictionData, expTag2, spark)
    data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_data20191204c")

    // 推送到校准数据表
    val resultDF = data
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")
      .cache()
    resultDF.show(10)

    resultDF
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly")

  }

  def assemblyData(dataRaw1: DataFrame, dataRaw2: DataFrame, dataRaw3: DataFrame, expTag: String, spark: SparkSession) = {
    val data1 = dataRaw1.filter(s"method = 'pred'").select("unitid", "exp_tag")

    val data2 = dataRaw2
      .withColumn("cvr_factor_baseline", col("cvr_factor"))
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor_baseline", "high_bid_factor", "low_bid_factor", "cpagiven", "exp_tag", "is_hidden")

    val data3 = dataRaw3
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(data1, Seq("unitid", "exp_tag"), "inner")
      .withColumn("cvr_factor_pred", col("cvr_factor"))
      .select("unitid", "exp_tag", "cvr_factor_pred")

    val data = data2
      .join(data3, Seq("unitid", "exp_tag"), "left_outer")
      .withColumn("cvr_factor", when(col("cvr_factor_pred").isNull, col("cvr_factor_baseline")).otherwise(col("cvr_factor_pred")))

    data
  }

  def getSelectionTable(date: String, hour: String, version: String, expTag: String, spark: SparkSession) = {
    val expTagSelection = getExpTags(expTag, spark)
    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  exp_tag,
         |  method
         |FROM
         |  dl_cpc.ocpc_calibration_method_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  $expTagSelection
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    data.show(10)
    data
  }

  def getPredictionData(date: String, hour: String, version: String, expTag: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |   cast(identifier as int) as unitid,
         |   media,
         |   1.0 / pred_pcoc as cvr_factor
         |FROM
         |    dl_cpc.ocpc_pcoc_prediction_result_hourly
         |WHERE
         |    `date` = '$date'
         |AND
         |    `hour` = '$hour'
         |AND
         |    version = '$version'
         |AND
         |    exp_tag = '$expTag'
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("media", udfMediaName()(col("media")))

    data
  }

  def getExpTags(expTag: String, spark: SparkSession) = {
    var editExpTag = expTag
    if (expTag == "base") {
      editExpTag = ""
    }
    val qtt = editExpTag + "Qtt"
    val midu = editExpTag + "MiDu"
    val hottopic = editExpTag + "HT66"
    val others = editExpTag + "Other"
    val result = s"exp_tag in ('$qtt', '$midu', '$hottopic', '$others')"
    result
  }

  def getBaselineData(date: String, hour: String, version: String, expTag: String, spark: SparkSession) = {
    val expTagSelection = getExpTags(expTag, spark)
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_pb_data_hourly_exp
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  $expTagSelection
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).cache()
    data.show(10)
    data
  }

}


