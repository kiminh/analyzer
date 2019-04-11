package com.cpc.spark.ocpcV3.HotTopicOcpc.model

import java.io.FileOutputStream

import com.typesafe.config.ConfigFactory
import ocpcCpcBid.ocpccpcbid.{OcpcCpcBidList, SingleOcpcCpcBid}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.model_v3.OcpcCvrSmooth.{savePbPack}

import scala.collection.mutable.ListBuffer

object OcpcCvrSmooth {
  def main(args: Array[String]): Unit = {
    /*
    计算重构后的cvr平滑数据:
    程序：
      ocpcV3.ocpc.filter.OcpcCvrSmooth
      ocpcV3.ocpc.filter.OcpcCPCbidV2
      identifier      string  NULL
      min_bid double  NULL
      cvr1    double  NULL
      cvr2    double  NULL
      cvr3    double  NULL
      min_cpm double  NULL
      factor1 double  NULL
      factor2 double  NULL
      factor3 double  NULL
      cpc_bid double  NULL
      cpa_suggest     double  NULL
      param_t double  NULL
      cali_value      double  NULL
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    val version = args(3).toString

    val conf = ConfigFactory.load("ocpc")
    val expDataPath = conf.getString("ocpc_all.ocpc_cpcbid.path_v2")
    val fileName = conf.getString("ocpc_all.ocpc_cpcbid.pbfile_v2")
    val smoothDataPath = conf.getString("ocpc_all.ocpc_cpcbid.factor_path")
    val suggestCpaPath = conf.getString("ocpc_all.ocpc_cpcbid.suggestcpa_path")
    println(s"cpcBid path is: $expDataPath")
    println(s"fileName is: $fileName")
    println(s"smooth factor path is $smoothDataPath")
    println(s"suggest cpa path is $suggestCpaPath")

    println("parameters:")
    println(s"date=$date, hour=$hour, media:$media, version:$version")

    // 获取postcvr数据
//    val cv1Data = getBaseData(media, "cvr1", hourInt, date, hour, spark)
//    val cv2Data = getBaseData(media, "cvr2", hourInt, date, hour, spark)
//    val cv3Data = getBaseData(media, "cvr3", hourInt, date, hour, spark)
    val cvr1 = getPostCvr(1, version, date, hour, spark)
    val cvr2 = getPostCvr(2, version, date, hour, spark)
    val cvr3 = getPostCvr(3, version, date, hour, spark)
//    val cvr1 = calculatePCOC(cv1Data, spark).withColumn("cvr1", col("post_cvr"))
//    val cvr2 = calculatePCOC(cv2Data, spark).withColumn("cvr2", col("post_cvr"))
//    val cvr3 = calculatePCOC(cv3Data, spark).withColumn("cvr3", col("post_cvr"))
    val cvrData = cvr1
      .join(cvr2, Seq("identifier"), "outer")
      .join(cvr3, Seq("identifier"), "outer")
      .select("identifier", "cvr1", "cvr2", "cvr3")
      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3"))

    // 获取cali_value
    val caliValue = getCaliValue(version, date, hour, spark)

    // 组装数据
    val result = assemblyData(cvrData, caliValue, spark)

    val resultDF = result
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))

    resultDF
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_post_cvr_unitid_hourly20190304")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_post_cvr_unitid_hourly")

    savePbPack(resultDF, fileName)

  }

  def getPostCvr(conversionGoal: Int, version: String, date: String, hour: String, spark: SparkSession) = {
    val cvrType = "cvr" + conversionGoal.toString
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  post_cvr
         |FROM
         |  dl_cpc.ocpc_pcoc_jfb_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  conversion_goal = $conversionGoal
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val resultDF = data
      .select("identifier", "post_cvr")
      .groupBy("identifier")
      .agg(avg(col("post_cvr")).alias(cvrType))
      .select("identifier", cvrType)

    resultDF
  }


  def assemblyData(cvrData: DataFrame, caliValue: DataFrame, spark: SparkSession) = {
    /*
      identifier      string  NULL
      min_bid double  NULL
      cvr1    double  NULL
      cvr2    double  NULL
      cvr3    double  NULL
      min_cpm double  NULL
      factor1 double  NULL
      factor2 double  NULL
      factor3 double  NULL
      cpc_bid double  NULL
      cpa_suggest     double  NULL
      param_t double  NULL
      cali_value      double  NULL
     */
    val data = cvrData
      .join(caliValue, Seq("identifier"), "left_outer")
      .withColumn("facotr1", lit(0.2))
      .withColumn("factor2", lit(0.5))
      .withColumn("factor3", lit(0.5))
      .withColumn("cpc_bid", lit(0.0))
      .withColumn("cpa_suggest", lit(0.0))
      .withColumn("param_t", lit(0))
      .select("identifier", "cvr1", "cvr2", "cvr3", "factor1", "factor2", "factor3", "cpc_bid", "cpa_suggest", "param_t", "cali_value")
      .withColumn("min_bid", lit(0))
      .withColumn("min_cpm", lit(0))
      .na.fill(0, Seq("min_bid", "min_cpm", "cpc_bid", "cpa_suggest", "param_t"))
      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3"))
      .na.fill(0.2, Seq("factor1"))
      .na.fill(0.5, Seq("factor2", "factor3"))
      .na.fill(1.0, Seq("cali_value"))
      .selectExpr("identifier", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3", "cast(min_cpm as double) as min_cpm", "cast(factor1 as double) factor1", "cast(factor2 as double) as factor2", "cast(factor3 as double) factor3", "cast(cpc_bid as double) cpc_bid", "cpa_suggest", "param_t", "cali_value")


    data
  }

  def getCaliValue(version: String, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  1.0 / pcoc as cali_value,
         |  jfb,
         |  kvalue,
         |  conversion_goal,
         |  version
         |FROM
         |  dl_cpc.ocpc_kvalue_smooth_strat
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)
    val result = rawData
      .select("identifier", "conversion_goal", "cali_value")
      .groupBy("identifier", "conversion_goal")
      .agg(avg(col("cali_value")).alias("cali_value"))
      .select("identifier", "cali_value")

    result
  }

}
