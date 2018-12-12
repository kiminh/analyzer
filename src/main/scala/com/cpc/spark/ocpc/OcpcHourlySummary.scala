package com.cpc.spark.ocpc

import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcHourlySummary {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString

    val spark = SparkSession
      .builder()
      .appName(s"OcpcHourlySummary: date=$date, hour=$hour")
      .enableHiveSupport()
      .getOrCreate()

    // 获取数据
    val data = joinData(date, hour, spark)
    data.write.mode("overwrite").saveAsTable("test.ocpc_data_summary_hourly")
  }

  def joinData(date: String, hour: String, spark: SparkSession) = {
//    val tableNameNoAPI = "dl_cpc.ocpc_check_hourly_report_noapi"
//    val tableNameAPI = "dl_cpc.ocpc_check_hourly_report_api"
//    val selectCondition = s"`date`='$date' and `hour`='$hour'"
//
//    val noAPI = spark
//      .table(tableNameNoAPI)
//      .where(selectCondition)
//      .withColumn("conversion_goal", lit(1))
//      .select("ideaid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "price", "avg_k", "recent_k")
//      .withColumn("cvr_cnt", when(col("cvr_cnt").isNull, 0).otherwise(col("cvr_cnt")))
//
//    val api = spark
//      .table(tableNameAPI)
//      .where(selectCondition)
//      .withColumn("conversion_goal", lit(2))
//      .select("ideaid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "price", "avg_k", "recent_k")
//      .withColumn("cvr_cnt", when(col("cvr_cnt").isNull, 0).otherwise(col("cvr_cnt")))


    val rawData = spark
      .table("test.ocpc_detail_report_hourly")
      .where(s"`date`='$date' and `hour`='$hour'")
      .select("user_id", "idea_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k")

    val ideaidList = spark
      .table("dl_cpc.ocpcv3_ctr_data_hourly")
      .where(s"`date`='$date' and `hour` <= '$hour' and media_appsid in ('80000001', '80000002')")
      .withColumn("idea_id", col("ideaid"))
      .withColumn("unit_id", col("unitid"))
      .select("idea_id", "unit_id")
      .distinct()

    val resultDF = rawData
      .join(ideaidList, Seq("idea_id"), "left_outer")
      .select("user_id", "idea_id", "unit_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k")

    resultDF.show(10)

    resultDF
  }
  
}
