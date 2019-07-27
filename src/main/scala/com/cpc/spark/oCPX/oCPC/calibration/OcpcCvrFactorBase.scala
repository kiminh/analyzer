package com.cpc.spark.oCPX.oCPC.calibration

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcCvrFactorBase {
  def main(args: Array[String]): Unit = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString
    val hourInt = args(4).toInt
    println("parameters:")
    println(s"date=$date, hour=$hour, expTag:$expTag, hourInt:$hourInt")

    val result = OcpcCvrFactorBaseMain(date, hour, version, expTag, hourInt, spark)
    result
      .repartition(10).write.mode("overwrite").saveAsTable("test.check_cvr_factor20190723a")
  }

  def OcpcCvrFactorBaseMain(date: String, hour: String, version: String, expTag: String, hourInt: Int, spark: SparkSession) = {
    /*
    动态计算alpha平滑系数
    1. 基于原始pcoc，计算预测cvr的量纲系数
    2. 二分搜索查找到合适的平滑系数
     */
    val baseData = getBaseData(hourInt, date, hour, spark)

    // 计算结果
    val result = calculatePCOC(baseData, spark)

    // 实验标签
    // min_cv:配置文件中如果为负数或空缺，则用默认值40，其他情况使用设定值
    val minCV = getExpConf(version, spark)

    val resultDF = result
      .select("unitid", "conversion_goal", "media", "cv", "pcoc")
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .join(minCV, Seq("conversion_goal", "exp_tag"), "left_outer")
      .na.fill(40, Seq("min_cv"))
      .filter(s"cv >= min_cv")
      .withColumn("version", lit(version))

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



  def calculatePCOC(rawData: DataFrame, spark: SparkSession) = {
    val data  =rawData
      .filter(s"isclick=1")
      .groupBy("unitid", "conversion_goal", "media")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("cv"),
        avg(col("exp_cvr")).alias("pre_cvr")
      )
      .withColumn("post_cvr", col("cv") * 1.0 / col("click"))
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .select("unitid", "conversion_goal", "media", "click", "cv", "pre_cvr", "post_cvr", "pcoc")

    data
  }

}