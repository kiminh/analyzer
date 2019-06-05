package com.cpc.spark.OcpcProtoType.suggest_cpa_novel_hourly

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.suggest_cpa_v2.OcpcSuggestCPA._

object OcpcSuggestCPA {
  def main(args: Array[String]): Unit = {
    /*
    新版推荐cpa程序：
    unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, cal_bid, auc, kvalue, industry, is_recommend, ocpc_flag, usertype, pcoc1, pcoc2

    主要源表：dl_cpc.ocpc_base_unionlog, dl_cpc.ocpc_label_cvr_hourly

    数据构成分为以下部分:
    1. 基础数据部分：unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, industry, usertype
    2. ocpc部分：kvalue
    3. 模型部分：auc
    4. 实时查询：ocpc_flag
    5. 历史推荐cpa数据：pcoc1, pcoc2
    6.
     */
    // 计算日期周期
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val conversionGoal = args(4).toInt
    val hourInt = args(5).toInt


    val spark = SparkSession
      .builder()
      .appName(s"ocpc suggest cpa v2: $date, $hour")
      .enableHiveSupport().getOrCreate()

    var cvrType = "cvr1"
    if (conversionGoal == 1) {
      cvrType = "cvr1"
    } else if (conversionGoal == 2) {
      cvrType = "cvr2"
    } else if (conversionGoal == 3) {
      cvrType = "cvr3"
    } else {
      cvrType = "cvr4"
    }
    println("parameters:")
    println(s"date=$date, hour=$hour, media=$media, conversionGoal=$conversionGoal, version=$version")


    // 取基础数据部分
    val baseData = getBaseData(media, conversionGoal, hourInt, date, hour, spark)

    // ocpc部分：kvalue
    val kvalue = getKvalue(media, hourInt, baseData, conversionGoal, date, hour, spark)

    // 模型部分
    val aucData = getAucData(version, conversionGoal, date, hour, spark)

    // 实时查询ocpc标记（从mysql抽取）
    val ocpcFlag = getOcpcFlag(conversionGoal, spark)

    // 历史推荐cpa的pcoc数据
    val prevData = getPrevSuggestData(version, conversionGoal, date, hour, spark)

    // 数据组装
    val result = assemblyData2(baseData, kvalue, aucData, ocpcFlag, prevData, conversionGoal, spark)

    val resultDF = result
      .withColumn("cv_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF.show(10)

    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_recommend_hourly_v2")
    println("successfully save data into table: dl_cpc.ocpc_suggest_cpa_recommend_hourly_v2")
  }

  def assemblyData2(baseData: DataFrame, kvalue: DataFrame, aucData: DataFrame, ocpcFlag: DataFrame, prevData: DataFrame, conversionGoal: Int, spark: SparkSession) = {
    /*
    assemlby the data together
     */
    val result = baseData
      .join(kvalue, Seq("unitid"), "left_outer")
      .join(aucData, Seq("unitid"), "left_outer")
      .join(ocpcFlag, Seq("unitid"), "left_outer")
      .join(prevData, Seq("unitid"), "left_outer")
      .select("unitid", "userid", "adclass", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "industry", "usertype", "kvalue_new","cal_bid_new", "auc", "is_ocpc", "pcoc1", "pcoc2")
      .withColumn("ocpc_flag", when(col("is_ocpc") === 1 && col("is_ocpc").isNotNull, 1).otherwise(0))
      .withColumn("cal_bid", col("cal_bid_new"))
      .withColumn("kvalue",col("kvalue_new"))
      .withColumn("is_recommend", when(col("auc").isNotNull && col("cal_bid").isNotNull && col("cvrcnt").isNotNull, 1).otherwise(0))
      .withColumn("is_recommend", when(col("auc") <= 0.65, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") < 0.7, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cal_bid") * 1.0 / col("acb") > 1.3, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("cvrcnt") < 10, 0).otherwise(col("is_recommend")))
      .withColumn("is_recommend", when(col("adclass")/1000 === 110110,1).otherwise(col("is_recommend")))
      .withColumn("zerobid_percent", lit(0.0))
      .withColumn("bottom_halfbid_percent", lit(0.0))
      .withColumn("top_halfbid_percent", lit(0.0))
      .withColumn("largebid_percent", lit(0.0))
      .withColumn("original_conversion", lit(conversionGoal))
      .withColumn("conversion_goal", lit(conversionGoal))
      .select("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc", "kvalue", "industry", "is_recommend", "ocpc_flag", "usertype", "pcoc1", "pcoc2", "zerobid_percent", "bottom_halfbid_percent", "top_halfbid_percent", "largebid_percent")

    result
  }
}
