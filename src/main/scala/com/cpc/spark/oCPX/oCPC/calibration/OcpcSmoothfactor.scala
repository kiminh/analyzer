package com.cpc.spark.oCPX.oCPC.calibration

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcSmoothfactor {
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
    val hourInt = args(4).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag, hourInt:$hourInt")

    val result = OcpcSmoothFactorMain(date, hour, version, expTag, hourInt, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_smooth_factor20190723a")
  }

  def OcpcSmoothFactorMain(date: String, hour: String, version: String, expTag: String, hourInt: Int, spark: SparkSession) = {
    val baseData = getBaseData(hourInt, date, hour, spark)

    // 计算结果
    val result = calculateCVR(baseData, spark)

    // 关联配置文件中的mincv和smooth_factor
    /*
    min_cv:配置文件中如果为负数或空缺，则用默认值10，其他情况使用设定值
    smooth_factor：配置文件中如果为负数或空缺，则用默认值(由udfSelectSmoothFactor函数决定)，其他情况使用设定值
     */
    val minCV = getExpConf(version, spark)

    val resultDF = result
      .select("unitid", "conversion_goal", "media", "click", "cv", "cvr")
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(minCV, Seq("conversion_goal", "exp_tag"), "left_outer")
      .withColumn("smooth_factor_back", udfSelectSmoothFactor()(col("conversion_goal")))
      .na.fill(10, Seq("min_cv"))
      .withColumn("smooth_factor", when(col("smooth_factor").isNotNull, col("smooth_factor")).otherwise(col("smooth_factor_back")))
      .withColumn("min_cv", udfSetMinCV()(col("min_cv")))
      .withColumn("smooth_factor", udfSetSmoothFactor()(col("smooth_factor")))
//      .filter(s"cv >= min_cv")
      .withColumn("version", lit(version))


    

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
      result = 10
    }
    result
  })


  def getExpConf(version: String, spark: SparkSession) ={
    // 从配置文件读取数据
    val conf = ConfigFactory.load("ocpc")
    val confPath = conf.getString("exp_config.jfb_factor")
    val rawData = spark.read.format("json").json(confPath)
    val data = rawData
      .filter(s"version = '$version'")
      .select("exp_tag", "conversion_goal", "min_cv", "smooth_factor")
      .distinct()

    println("smooth factor: config")
    data.show(10)

    data
  }

  def calculateCVR(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("unitid", "conversion_goal", "media")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv")
      )
      .withColumn("cvr", col("cv") * 1.0 / col("click"))
      .select("unitid", "conversion_goal", "media", "click", "cv", "cvr")

    val result = data
        .select("unitid", "conversion_goal", "media", "click", "cv", "cvr")

    result
  }
}