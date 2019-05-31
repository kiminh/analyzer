package com.cpc.spark.OcpcProtoType.model_qtt

import java.io.FileOutputStream

import com.typesafe.config.ConfigFactory
import ocpcCpcBid.ocpccpcbid.{OcpcCpcBidList, SingleOcpcCpcBid}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.model_v4.OcpcCvrSmoothV2._

import scala.collection.mutable.ListBuffer

object OcpcCvrSmoothV2 {
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
    val version = args(2).toString
    val media = args(3).toString

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
    val suggestCPA = getCPAsuggest(suggestCpaPath, date, hour, spark)

    // 获取cali_value
    val caliValue = getCaliValueV2(version, date, hour, spark)

    // 组装数据
    val result = assemblyData(cvrData, factorData, expData, suggestCPA, caliValue, spark)

    val resultDF = result
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_post_cvr_unitid_hourly")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_post_cvr_unitid_hourly")

    savePbPack(resultDF, fileName)

  }

  def getCaliValueV2(version: String, date: String, hour: String, spark: SparkSession) = {
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
      .withColumn("cali_value", when(col("cali_value") < 0.1, 0.1).otherwise(col("cali_value")))
      .withColumn("cali_value", when(col("cali_value") > 3.0, 3.0).otherwise(col("cali_value")))

    result
  }

}
