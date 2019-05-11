package com.cpc.spark.OcpcProtoType.model_qtt

import java.io.FileOutputStream

import com.typesafe.config.ConfigFactory
import ocpcCpcBid.ocpccpcbid.{OcpcCpcBidList, SingleOcpcCpcBid}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.model_v3.OcpcCvrSmooth._
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
    val cvr1 = getPostCvr(1, version, date, hour, spark)
    val cvr2 = getPostCvr(2, version, date, hour, spark)
    val cvr3 = getPostCvr(3, version, date, hour, spark)
    val cvrData = cvr1
      .join(cvr2, Seq("identifier"), "outer")
      .join(cvr3, Seq("identifier"), "outer")
      .select("identifier", "cvr1", "cvr2", "cvr3")
      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3"))

    // 获取factor数据
    val factorData = getCvrAlphaData(smoothDataPath, date, hour, spark)

    // 获取cpc_bid数据
    val expData = getCpcBidData(expDataPath, date, hour, spark)

    // 获取cpa_suggest和param_t数据
    val suggestCPA = getCPAsuggestV4(suggestCpaPath, date, hour, spark)

    // 获取cali_value
    val caliValue = getCaliValue(date, hour, spark)

    // 组装数据
    val result = assemblyDataV2(cvrData, factorData, expData, suggestCPA, caliValue, spark)

    val resultDF = result
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))

    resultDF
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_post_cvr_unitid_hourly")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_post_cvr_unitid_hourly")

    savePbPack(resultDF, fileName)

  }

  def getCPAsuggestV4(suggestCpaPath: String, date: String, hour: String, spark: SparkSession) = {
    /*
    两条来源：
    1. 从mysql读取实时正在跑ocpc的广告单元和对应转化目标，按照unitid和conversion_goal设置对应推荐cpa
    2. 从配置文件读取推荐cpa
    3. 数据关联，优先配置文件的推荐cpa
     */

    // 从推荐cpa表中读取：dl_cpc.ocpc_suggest_cpa_k_once
    val sqlRequest =
      s"""
         |SELECT
         |  cast(unitid as string) identifier,
         |  cpa * 100 as cpa_suggest,
         |  2 as param_t
         |FROM
         |  test.ocpc_qtt_light_control_v2
       """.stripMargin
    println(sqlRequest)
    val resultDF  = spark
      .sql(sqlRequest)
      .groupBy("identifier")
      .agg(
        min(col("cpa_suggest")).alias("cpa_suggest"),
        min(col("param_t")).alias("param_t")
      )
      .select("identifier", "cpa_suggest", "param_t")

    resultDF
  }

  def assemblyDataV2(cvrData: DataFrame, factorData: DataFrame, expData: DataFrame, suggestCPA: DataFrame, caliValue: DataFrame, spark: SparkSession) = {
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
    val result = cvrData
      .join(factorData, Seq("identifier"), "outer")
      .join(expData, Seq("identifier"), "outer")
      .join(suggestCPA, Seq("identifier"), "outer")
      .join(caliValue, Seq("identifier"), "left_outer")
      .select("identifier", "cvr1", "cvr2", "cvr3", "factor1", "factor2", "factor3", "cpc_bid", "cpa_suggest", "param_t", "cali_value")
      .withColumn("min_bid", lit(0))
      .withColumn("min_cpm", lit(0))
      .na.fill(0, Seq("min_bid", "min_cpm", "cpc_bid", "cpa_suggest", "param_t"))
      .na.fill(0.0, Seq("cvr1", "cvr2", "cvr3"))
      .na.fill(0.2, Seq("factor1"))
      .na.fill(0.5, Seq("factor2", "factor3"))
      .na.fill(1.0, Seq("cali_value"))
      .withColumn("factor3", when(col("identifier") === "2041214", 0.3).otherwise(col("factor3")))
      .selectExpr("identifier", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3", "cast(min_cpm as double) as min_cpm", "cast(factor1 as double) factor1", "cast(factor2 as double) as factor2", "cast(factor3 as double) factor3", "cast(cpc_bid as double) cpc_bid", "cpa_suggest", "param_t", "cali_value")

//    // 如果cali_value在1/1.3到1.3之间，则factor变成0.2
//    val result = data
//        .withColumn("factor3", udfSetFactor3()(col("factor3"), col("cali_value")))

    result
  }



}
