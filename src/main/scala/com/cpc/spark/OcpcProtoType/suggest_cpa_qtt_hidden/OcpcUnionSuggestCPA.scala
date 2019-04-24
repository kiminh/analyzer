package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt_hidden

import com.cpc.spark.OcpcProtoType.model_v3.OcpcSmoothFactor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.suggest_cpa_v1.OcpcUnionSuggestCPA._
import org.apache.log4j.{Level, Logger}


object OcpcUnionSuggestCPA {
  def main(args: Array[String]): Unit = {
    /*
    将qtt_demo的三种转化目标的表union到一起
     */
    // 计算日期周期
    Logger.getRootLogger.setLevel(Level.WARN)
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
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_suggest_cpa_recommend_hourly20190411")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_recommend_hourly")
    println("successfully save data into table: dl_cpc.ocpc_suggest_cpa_recommend_hourly")

  }

  def getRecommendLabelV2(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |  *,
         |  (case when unitid in (2078340, 2078121, 2077145, 2072348, 2024103, 1988046, 2064875, 2040762, 2064725) then 50
         |        else 60 end) as cv_threshold
         |FROM
         |  base_data
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    val resultDF = data
      .withColumn("is_recommend_old", col("is_recommend"))
      .withColumn("is_recommend", when(col("auc").isNotNull && col("cal_bid").isNotNull && col("cvrcnt").isNotNull, 1).otherwise(0))
      .withColumn("is_recommend", when(col("auc") <= 0.65, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") < 0.7, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") > 1.3, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cvrcnt") < col("cv_threshold"), 0).otherwise(col("is_recommend")))
    resultDF.write.mode("overwrite").saveAsTable("test.test_check_data20190424a")

    resultDF
  }

}
