package com.cpc.spark.oCPX.oCPC.calibration

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.oCPX.oCPC.calibration.OcpcCalibrationBase.OcpcCalibrationBaseMainOnlySmooth
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcSmoothfactorV2 {
  def main(args: Array[String]): Unit = {
    /*
    基于最近一段时间的后验cvr计算平滑用的后验cvr
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

    val dataRaw1 = OcpcCalibrationBaseMainOnlySmooth(date, hour, hourInt1, spark).cache()
    val dataRaw2 = OcpcCalibrationBaseMainOnlySmooth(date, hour, hourInt2, spark).cache()
    val dataRaw3 = OcpcCalibrationBaseMainOnlySmooth(date, hour, hourInt3, spark).cache()

    val result = OcpcSmoothfactorMain(date, hour, version, expTag, dataRaw1, dataRaw2, dataRaw3, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_smooth_factor20190723b")
  }

  def OcpcSmoothfactorMain(date: String, hour: String, version: String, expTag: String, dataRaw1: DataFrame, dataRaw2: DataFrame, dataRaw3: DataFrame, spark: SparkSession) = {
    // smooth实验配置文件
    // min_cv:配置文件中如果为负数或空缺，则用默认值0，其他情况使用设定值
    // smooth_factor：配置文件中如果为负数或空缺，则用默认值(由udfSelectSmoothFactor函数决定)，其他情况使用设定值
    val expConf = getExpConf(version, spark)

    val data1 = dataRaw1
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(0, Seq("min_cv"))
      .withColumn("min_cv", udfSetMinCV()(col("min_cv")))
      .filter(s"cv > 0")
    data1.show(10)

    val data2 = dataRaw2
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(0, Seq("min_cv"))
      .withColumn("min_cv", udfSetMinCV()(col("min_cv")))
      .filter(s"cv > 0")
    data2.show(10)

    val data3 = dataRaw3
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(0, Seq("min_cv"))
      .withColumn("min_cv", udfSetMinCV()(col("min_cv")))
      .filter(s"cv > 0")
    data3.show(10)

    // 计算最终值
    val calibration1 = calculateCalibrationValue(data1, data2, spark)
    val calibrationNew = data3
      .withColumn("cvr_new", col("post_cvr"))
      .select("unitid", "conversion_goal", "exp_tag", "cvr_new")

    val calibration = calibrationNew
      .join(calibration1, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
      .withColumn("cvr", when(col("cvr3").isNotNull, col("cvr3")).otherwise(col("cvr_new")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(0, Seq("min_cv"))
      .withColumn("min_cv", udfSetMinCV()(col("min_cv")))
      .withColumn("smooth_factor_back", udfSelectSmoothFactor()(col("conversion_goal")))
      .withColumn("smooth_factor", when(col("smooth_factor").isNotNull, col("smooth_factor")).otherwise(col("smooth_factor_back")))
      .withColumn("smooth_factor", udfSetSmoothFactor()(col("smooth_factor")))
      .cache()

    calibration.show(10)
//    calibration
//      .repartition(10).write.mode("overwrite").saveAsTable("test.check_smooth_factor20190723a")

    val resultDF = calibration
      .withColumn("version", lit(version))
      .select("unitid", "conversion_goal", "exp_tag", "version", "cvr", "smooth_factor")

    resultDF
  }

  def udfSelectSmoothFactor() = udf((conversionGoal: Int) => {
    var factor = conversionGoal match {
      case 1 => 0.2
      case 2 => 0.5
      case 3 => 0.5
      case 4 => 0.2
      case _ => 0.0
    }
    factor
  })


  def udfSetSmoothFactor() = udf((smoothFactor: Double) => {
    var result = smoothFactor
    if (result < 0) {
      result = 0.5
    }
    result
  })

  def udfSetMinCV() = udf((minCV: Int) => {
    var result = minCV
    if (result < 0) {
      result = 0
    }
    result
  })


  def getExpConf(version: String, spark: SparkSession) ={
    // 从配置文件读取数据
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_config.smooth_factor")
    val rawData = spark.read.format("json").json(confPath)
    val data = rawData
      .filter(s"version = '$version'")
      .select("exp_tag", "conversion_goal", "min_cv", "smooth_factor")
      .distinct()

    println("smooth factor: config")
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
      .withColumn("cvr1", col("post_cvr"))
      .select("unitid", "conversion_goal", "exp_tag", "cvr1")

    // 备用校准模型
    val data2 = dataRaw2
      .filter(s"cv >= min_cv")
      .withColumn("cvr2", col("post_cvr"))
      .select("unitid", "conversion_goal", "exp_tag", "cvr2")

    // 数据表关联
    val data = data2
      .join(data1, Seq("unitid", "conversion_goal", "exp_tag"), "left_outer")
      .na.fill(0, Seq("flag"))
      .withColumn("cvr3", when(col("cvr1").isNotNull, col("cvr1")).otherwise(col("cvr2")))

    data.show()

    data

  }
}