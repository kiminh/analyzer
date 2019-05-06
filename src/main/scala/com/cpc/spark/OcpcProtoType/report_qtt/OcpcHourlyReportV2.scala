package com.cpc.spark.OcpcProtoType.report_qtt

import com.cpc.spark.tools.OperateMySQL
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.cpc.spark.OcpcProtoType.report.OcpcHourlyReport._
import org.apache.log4j.{Level, Logger}

object OcpcHourlyReportV2 {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    /*
    新版报表程序
    1. 从ocpc_unionlog拉取ocpc广告记录
    2. 采用数据关联方式获取转化数据
    3. 统计分ideaid级别相关数据
    4. 统计分conversion_goal级别相关数据
    5. 存储到hdfs
    6. 存储到mysql
     */
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media")

    // 拉取点击、消费、转化等基础数据
    var isHidden = 0
    if (version == "qtt_demo") {
      isHidden = 0
    } else {
      isHidden = 1
    }
    val baseData = getBaseData(media, date, hour, spark).filter(s"is_hidden = $isHidden")

    // 分ideaid和conversion_goal统计数据
    val rawDataUnit = preprocessDataByUnit(baseData, date, hour, spark)
    val dataUnit = getDataByUnit(rawDataUnit, version, date, hour, spark)

    // 分conversion_goal统计数据
    val rawDataConversion = preprocessDataByConversion(dataUnit, date, hour, spark)
    val costDataConversion = preprocessCostByConversion(dataUnit, date, hour, spark)
    val dataConversion = getDataByConversion(rawDataConversion, version, costDataConversion, date, hour, spark)

    // 存储数据到hadoop
    saveDataToHDFS(dataUnit, dataConversion, version, date, hour, spark)



  }

  def saveDataToHDFS(dataUnit: DataFrame, dataConversion: DataFrame, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    存储unitid级别和conversion_goal级别的报表到hdfs
     */
    dataUnit
      .withColumn("identifier", col("unitid"))
      .selectExpr("cast(identifier as string) identifier", "userid", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "date", "hour")
      .withColumn("version", lit(version))
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_detail_report_hourly_v4_20190413")
    //      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_detail_report_hourly_v4")

    dataUnit
      .withColumn("identifier", col("unitid"))
      .selectExpr("cast(identifier as string) identifier", "userid", "conversion_goal", "cali_value", "cali_pcvr", "cali_postcvr", "smooth_factor", "cpa_suggest", "hourly_expcvr", "hourly_calivalue", "hourly_calipcvr", "hourly_calipostcvr", "date", "hour")
      .withColumn("version", lit(version))
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_cali_detail_report_hourly_20190413")
    //      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_cali_detail_report_hourly")

    dataConversion
      .selectExpr("conversion_goal", "total_adnum", "step2_adnum", "low_cpa_adnum", "high_cpa_adnum", "step2_cost", "step2_cpa_high_cost", "impression", "click", "conversion", "ctr", "click_cvr", "cost", "acp", "pre_cvr", "post_cvr", "q_factor", "acb", "auc", "cpa_given", "cpa_real", "cpa_ratio", "date", "hour")
      .withColumn("version", lit(version))
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_summary_report_hourly_v3_20190413")
    //      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_summary_report_hourly_v4")
  }



}