package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt_hidden

import com.cpc.spark.OcpcProtoType.model_v3.OcpcSmoothFactor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.suggest_cpa_v1.OcpcUnionSuggestCPA._


object OcpcUnionSuggestCPA {
  def main(args: Array[String]): Unit = {
    /*
    将qtt_demo的三种转化目标的表union到一起
     */
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val spark = SparkSession
      .builder()
      .appName(s"ocpc suggest cpa v2: $date, $hour, $version")
      .enableHiveSupport().getOrCreate()

    val baseResult = getSuggestData("qtt_hidden", date, hour, spark)
    val cvr1Cali = getNewCali(media, baseResult, 1, 48, date, hour, spark)
    val cvr2Cali = getNewCali(media, baseResult, 2, 48, date, hour, spark)
    val cvr3Cali = getNewCali(media, baseResult, 3, 48, date, hour, spark)

    val cvrCali = cvr1Cali.union(cvr2Cali).union(cvr3Cali)
//    cvrCali.write.mode("overwrite").saveAsTable("test.check_ocpc_new_calidata20190411")

    val updateData = baseResult
      .join(cvrCali, Seq("unitid", "conversion_goal"), "left_outer")
      .withColumn("kvalue", when(col("kvalue_new").isNotNull, col("kvalue_new")).otherwise(col("kvalue_old")))
      .withColumn("cal_bid", when(col("cal_bid_new").isNotNull, col("cal_bid_new")).otherwise(col("cal_bid_old")))

    val result = getRecommendLabel(updateData, date, hour, spark)

    val resultDF = result
      .select("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc", "kvalue", "industry", "is_recommend", "ocpc_flag", "usertype", "pcoc1", "pcoc2", "zerobid_percent", "bottom_halfbid_percent", "top_halfbid_percent", "largebid_percent")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_suggest_cpa_recommend_hourly20190411")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_recommend_hourly")
    println("successfully save data into table: dl_cpc.ocpc_suggest_cpa_recommend_hourly")

  }

//  def getRecommendLabel(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
//    val resultDF = data
//      .withColumn("is_recommend_old", col("is_recommend"))
//      .withColumn("is_recommend", when(col("auc").isNotNull && col("cal_bid").isNotNull && col("cvrcnt").isNotNull, 1).otherwise(0))
//      .withColumn("is_recommend", when(col("auc") <= 0.65, 0).otherwise(col("is_recommend")))
//      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") < 0.7, 0).otherwise(col("is_recommend")))
//      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") > 1.3, 0).otherwise(col("is_recommend")))
//      .withColumn("is_recommend", when(col("cvrcnt") < 60, 0).otherwise(col("is_recommend")))
//
//    resultDF
//  }
//
//  def getNewCali(media: String, suggestData: DataFrame, conversionGoal: Int, hourInt: Int, date: String, hour: String, spark: SparkSession) = {
//    var cvrType = "cvr" + conversionGoal.toString
//    val baseData = OcpcSmoothFactor.getBaseData(media, cvrType, hourInt, date, hour, spark)
//    val rawData = OcpcSmoothFactor.calculateSmooth(baseData, spark)
//    rawData.createOrReplaceTempView("raw_data")
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  cast(identifier as int) unitid,
//         |  1.0 / pcoc as cali_value,
//         |  1.0 / jfb as kvalue,
//         |  post_cvr
//         |FROM
//         |  raw_data
//       """.stripMargin
//    println(sqlRequest)
//    val cvrData = spark.sql(sqlRequest)
//
//    val data = baseData
//      .join(cvrData, Seq("unitid"), "inner")
//      .withColumn("pre_cvr", col("exp_cvr") * 0.5 * col("cali_value") + col("post_cvr") * 0.5)
//      .select("searchid", "unitid", "pre_cvr")
//      .groupBy("unitid")
//      .agg(avg(col("pre_cvr")).alias("pre_cvr"))
//      .select("unitid", "pre_cvr")
//
//    val result = data
//      .join(cvrData, Seq("unitid"), "inner")
//      .withColumn("conversion_goal", lit(conversionGoal))
//      .select("unitid", "conversion_goal", "pre_cvr", "kvalue")
//
//    val resultDF = suggestData
//      .join(result, Seq("unitid", "conversion_goal"), "inner")
//      .select("unitid", "conversion_goal", "cpa", "pre_cvr", "kvalue")
//      .withColumn("cal_bid_new", col("cpa") * col("pre_cvr") * col("kvalue"))
//      .withColumn("kvalue_new", col("kvalue"))
//      .select("unitid", "conversion_goal", "cal_bid_new", "kvalue_new")
//
//    resultDF
//
//  }
//
//  def getSuggestData(version: String, date: String, hour: String, spark: SparkSession) = {
//    val sqlRequest =
//      s"""
//         |SELECT
//         |  *
//         |FROM
//         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly_v2
//         |WHERE
//         |  `date` = '$date'
//         |AND
//         |  `hour` = '$hour'
//         |AND
//         |  version = '$version'
//       """.stripMargin
//    println(sqlRequest)
//    val data = spark.sql(sqlRequest)
//    //    data.write.mode("overwrite").saveAsTable("test.check_suggest_cpa_data20190327")
//
//    val resultDF = data
//      .withColumn("kvalue_old", col("kvalue"))
//      .withColumn("cal_bid_old", col("cal_bid"))
//      .select("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid_old", "auc", "kvalue_old", "industry", "is_recommend", "ocpc_flag", "usertype", "pcoc1", "pcoc2", "zerobid_percent", "bottom_halfbid_percent", "top_halfbid_percent", "largebid_percent")
//
//    resultDF
//
//  }
}
