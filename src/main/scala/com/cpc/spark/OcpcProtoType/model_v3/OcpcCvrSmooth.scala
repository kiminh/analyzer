package com.cpc.spark.OcpcProtoType.model_v3

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.model_v3.OcpcSmoothFactor.{getBaseData, calculatePCOC}

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
    val hourInt = args(3).toInt
    val version = args(4).toString

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
    println(s"date=$date, hour=$hour, media:$media, hourInt:$hourInt, version:$version")

    // 获取postcvr数据
    val cv1Data = getBaseData(media, "cvr1", hourInt, date, hour, spark)
    val cv2Data = getBaseData(media, "cvr2", hourInt, date, hour, spark)
    val cv3Data = getBaseData(media, "cvr3", hourInt, date, hour, spark)
    val cvr1 = calculatePCOC(cv1Data, spark).withColumn("cvr1", col("post_cvr"))
    val cvr2 = calculatePCOC(cv2Data, spark).withColumn("cvr2", col("post_cvr"))
    val cvr3 = calculatePCOC(cv3Data, spark).withColumn("cvr3", col("post_cvr"))
    val cvrData = cvr1
      .join(cvr2, Seq("unitid"), "outer")
      .join(cvr3, Seq("unitid"), "outer")
      .selectExpr("cast(unitid as string) identifier", "cvr1", "cvr2", "cvr3")
      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3"))

    // 获取factor数据
    val factorData = getCvrAlphaData(smoothDataPath, date, hour, spark)

    // 获取cpc_bid数据
    val expData = getCpcBidData(expDataPath, date, hour, spark)

    // 获取cpa_suggest和param_t数据

    // 获取cali_value

  }

  def getCpcBidData(dataPath: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(dataPath)

    val resultDF = data
      .groupBy("identifier")
      .agg(
        min(col("cpc_bid")).alias("cpc_bid")
      )
      .select("identifier", "cpc_bid")

    resultDF.show(10)
    resultDF
  }

  def getCvrAlphaData(dataPath: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(dataPath)

    val resultDF = data
      .groupBy("unitid")
      .agg(
        min(col("factor1")).alias("factor1"),
        min(col("factor2")).alias("factor2"),
        min(col("factor3")).alias("factor3")
      )
      .withColumn("identifier", col("unitid"))
      .selectExpr("cast(identifier as string) identifier", "factor1", "factor2", "factor3")

    resultDF.show(10)
    resultDF
  }


}
