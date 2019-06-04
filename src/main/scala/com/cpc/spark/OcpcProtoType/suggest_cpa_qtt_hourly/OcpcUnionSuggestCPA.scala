package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt_hourly

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.suggest_cpa_v2.OcpcUnionSuggestCPA._


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

    val baseResult = getSuggestData(version, date, hour, spark)

    val resultDF = baseResult
      .select("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc", "kvalue", "industry", "is_recommend", "ocpc_flag", "usertype", "pcoc1", "pcoc2", "zerobid_percent", "bottom_halfbid_percent", "top_halfbid_percent", "largebid_percent")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_suggest_cpa_recommend_hourly20190411")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_recommend_hourly")
    println("successfully save data into table: dl_cpc.ocpc_suggest_cpa_recommend_hourly")

  }

}
