package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.{Level, Logger}
import com.cpc.spark.OcpcProtoType.suggest_cpa_v1.OcpcUnionSuggestCPA._


object OcpcUnionSuggestCPAv2 {
  def main(args: Array[String]): Unit = {
    /*
    将qtt_demo的三种转化目标的表union到一起
     */
    // 计算日期周期
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val spark = SparkSession
      .builder()
      .appName(s"ocpc suggest cpa v2: $date, $hour, $version")
      .enableHiveSupport().getOrCreate()

    val baseResult = getSuggestData(version, date, hour, spark)
//    val cvr2Cali = getNewCali(baseResult, date, hour, spark)
    val cvr1Cali = getNewCali(baseResult, 1, 48, date, hour, spark)
    val cvr2Cali = getNewCali(baseResult, 2, 48, date, hour, spark)
    val cvr3Cali = getNewCali(baseResult, 3, 48, date, hour, spark)

    val cvrCali = cvr1Cali.union(cvr2Cali).union(cvr3Cali)


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
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_recommend_hourly")
    println("successfully save data into table: dl_cpc.ocpc_suggest_cpa_recommend_hourly")

  }

}
