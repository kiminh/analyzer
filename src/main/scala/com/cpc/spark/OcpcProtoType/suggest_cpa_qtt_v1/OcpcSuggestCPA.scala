package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt_v1

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.suggest_cpa.OcpcSuggestCPA._


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
    val media = args(2).toString
    val conversionGoal = args(3).toInt
    val version = args(4).toString


    val spark = SparkSession
      .builder()
      .appName(s"ocpc suggest cpa v2: $date, $hour")
      .enableHiveSupport().getOrCreate()

    var cvrType = "cvr1"
    if (conversionGoal == 1) {
      cvrType = "cvr1"
    } else if (conversionGoal == 2) {
      cvrType = "cvr2"
    } else {
      cvrType = "cvr3"
    }
    println("parameters:")
    println(s"date=$date, hour=$hour, media=$media, conversionGoal=$conversionGoal, version=$version")


    // 取基础数据部分
    val baseData = getBaseData(media, conversionGoal, 72, date, hour, spark)

    // ocpc部分：kvalue
    val kvalue = getKvalue(version, conversionGoal, date, hour, spark)

    // 模型部分
    val aucData = getAucData(version, conversionGoal, date, hour, spark)

    // 实时查询ocpc标记（从mysql抽取）
    val ocpcFlag = getOcpcFlag(conversionGoal, spark)

    // 历史推荐cpa的pcoc数据
    val prevData = getPrevSuggestData(version, conversionGoal, date, hour, spark)

    // 数据组装
    val result = assemblyData(baseData, kvalue, aucData, ocpcFlag, prevData, conversionGoal, spark)

    val resultDF = result
      .withColumn("cv_goal", lit(conversionGoal))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF.show(10)

//    resultDF.write.mode("overwrite").saveAsTable("test.check_suggest_data20190307a")
    resultDF
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_suggest_cpa_recommend_hourly_v2")
    println("successfully save data into table: dl_cpc.ocpc_suggest_cpa_recommend_hourly_v2")
  }


}
