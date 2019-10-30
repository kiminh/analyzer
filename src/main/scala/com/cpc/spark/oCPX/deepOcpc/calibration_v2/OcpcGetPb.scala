package com.cpc.spark.oCPX.deepOcpc.calibration_v2

import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcCVRfactor._
import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcCalibrationBase._
import com.cpc.spark.oCPX.deepOcpc.calibration_tools.OcpcJFBfactor._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.deepOcpc.calibration.OcpcGetPb_daily.udfSetSmoothFactor
import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcShallowFactor._
import com.cpc.spark.oCPX.deepOcpc.calibration_v2.OcpcRetentionFactor._


object OcpcGetPb {
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
    val hourInt = args(4).toInt
    val minCV1 = args(5).toInt
    val minCV2 = args(6).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt:$hourInt")

    // 计算计费比系数、后验激活转化率、先验点击次留率
    val data1 = OcpcShallowFactorMain(date, hour, hourInt, expTag, minCV1, spark)
    data1
      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191029a")

    // 计算自然天激活次留率
    val data2 = OcpcRetentionFactorMain(date, expTag,minCV2, spark)
    data2
      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191029b")


    // 计算cvr校准系数
    val data = calculateCalibrationValue(data1, data2, spark)
    data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_deep_cvr20191029c")


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
      .repartition(1)
      .write.mode("overwrite").insertInto("test.ocpc_deep_pb_data_hourly")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_pb_data_hourly")
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


  def calculateCalibrationValue(dataRaw1: DataFrame, dataRaw2: DataFrame, spark: SparkSession) = {
    val data1 = dataRaw1.filter(s"cv >= min_cv")
    val data2 = dataRaw2.filter(s"cv2 >= min_cv")
    val data = data1
      .join(data2, Seq("unitid", "media"), "inner")
      .select("unitid", "deep_conversion_goal", "media", "exp_tag", "jfb", "pre_cvr", "post_cvr", "deep_cvr")
      .withColumn("jfb_factor", lit(1.0) / col("jfb"))
      .withColumn("cvr_factor1", col("post_cvr") * 1.0 / col("pre_cvr"))
      .withColumn("cvr_factor2", (col("pre_cvr1") * col("deep_cvr")) * 1.0 / col("pre_cvr2"))
      .withColumn("cvr_factor", col("cvr_factor1") * col("cvr_factor2"))
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


}
