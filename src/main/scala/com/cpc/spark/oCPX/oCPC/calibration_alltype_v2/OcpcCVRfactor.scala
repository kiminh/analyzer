package com.cpc.spark.oCPX.oCPC.calibration_alltype_v2

import com.cpc.spark.oCPX.OcpcTools.{udfMediaName, udfSetExpTag}
import com.cpc.spark.oCPX.oCPC.calibration_alltype.OcpcCalibrationBase._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcCVRfactor {
  def main(args: Array[String]): Unit = {
    /*
    校准策略：
    主校准 + 备用校准：
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    // 计算日期周期
    // bash: 2019-01-02 12 1 qtt_demo qtt
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

    val dataRaw1 = OcpcCalibrationBaseMain(date, hour, hourInt1, spark).cache()
    val dataRaw2 = OcpcCalibrationBaseMain(date, hour, hourInt2, spark).cache()
    val dataRaw3 = OcpcCalibrationBaseMain(date, hour, hourInt3, spark).cache()

    val result = OcpcCVRfactorMain(date, hour, version, expTag, dataRaw1, dataRaw2, dataRaw3, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_cvr_factor20190723b")

  }

  def OcpcCVRfactorMain(date: String, hour: String, version: String, expTag: String, dataRaw1: DataFrame, dataRaw2: DataFrame, dataRaw3: DataFrame, spark: SparkSession) = {
    // cvr实验配置文件
    // min_cv:配置文件中如果为负数或空缺，则用默认值40，其他情况使用设定值
    val expConf = getExpConf(version, spark)

    val data1 = dataRaw1
        .withColumn("media", udfMediaName()(col("media")))
        .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
        .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
        .na.fill(40, Seq("min_cv"))
        .filter(s"cv > 0")
    data1.show(10)

    val data2 = dataRaw2
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(40, Seq("min_cv"))
      .filter(s"cv > 0")
    data2.show(10)

    val data3 = dataRaw3
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(expConf, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(40, Seq("min_cv"))
      .filter(s"cv > 0")
    data3.show(10)

    // 计算最终值
    val calibration1 = calculateCalibrationValue(data1, data2, spark)
    val calibrationNew = data3
      .filter(s"cv >= min_cv")
      .withColumn("pcoc_new", col("pcoc"))
      .select("identifier", "conversion_goal", "exp_tag", "pcoc_new")

    val calibration = calibrationNew
      .join(calibration1, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .withColumn("pcoc", when(col("pcoc3").isNotNull, col("pcoc3")).otherwise(col("pcoc_new")))
      .cache()

    calibration.show(10)
//    calibration
//      .repartition(10).write.mode("overwrite").saveAsTable("test.check_cvr_factor20190723a")


    val resultDF = calibration
      .withColumn("version", lit(version))
      .select("identifier", "conversion_goal", "exp_tag", "version", "pcoc")

    resultDF

  }

  def getExpConf(version: String, spark: SparkSession) ={
    // 从配置文件读取数据
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_config.cvr_factor")
    val rawData = spark.read.format("json").json(confPath)
    val data = rawData
      .filter(s"version = '$version'")
      .select("exp_tag", "conversion_goal", "min_cv")
      .distinct()

    println("cvr factor config:")
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
      .withColumn("pcoc1", col("pcoc"))
      .select("identifier", "conversion_goal", "exp_tag", "pcoc1")

    // 备用校准模型
    val data2 = dataRaw2
      .filter(s"cv >= min_cv")
      .withColumn("pcoc2", col("pcoc"))
      .select("identifier", "conversion_goal", "exp_tag", "pcoc2")

    // 数据表关联
    val data = data2
      .join(data1, Seq("identifier", "conversion_goal", "exp_tag"), "left_outer")
      .na.fill(0, Seq("flag"))
      .withColumn("pcoc3", when(col("pcoc1").isNotNull, col("pcoc1")).otherwise(col("pcoc2")))

    data.show()

    data

  }



}


