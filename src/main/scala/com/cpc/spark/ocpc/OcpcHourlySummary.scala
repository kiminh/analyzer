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
    val tableNameNoAPI = "dl_cpc.ocpc_check_hourly_report_noapi"
    val tableNameAPI = "dl_cpc.ocpc_check_hourly_report_api"
    val selectCondition = s"`date`='$date' and `hour`='$hour'"

    val noAPI = spark
      .table(tableNameNoAPI)
      .where(selectCondition)
      .withColumn("conversion_goal", lit(1))
      .select("ideaid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "price", "avg_k", "recent_k")
      .withColumn("cvr_cnt", when(col("cvr_cnt").isNull, 0).otherwise(col("cvr_cnt")))

    val api = spark
      .table(tableNameAPI)
      .where(selectCondition)
      .withColumn("conversion_goal", lit(2))
      .select("ideaid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "price", "avg_k", "recent_k")
      .withColumn("cvr_cnt", when(col("cvr_cnt").isNull, 0).otherwise(col("cvr_cnt")))

    val ideaidList = spark
      .table("dl_cpc.ocpcv3_ctr_data_hourly")
      .where(s"`date`='$date' and `hour` <= '$hour' and media_appsid in ('80000001', '80000002')")
      .select("ideaid", "unitid")
      .distinct()

    val resultDF = noAPI
      .union(api)
      .join(ideaidList, Seq("ideaid", "userid"), "left_outer")
      .select("ideaid", "unitid", "userid", "conversion_goal", "step2_percent", "cpa_given", "cpa_real", "show_cnt", "ctr_cnt", "cvr_cnt", "price", "avg_k", "recent_k")

    resultDF.show(10)

    resultDF
  }

  def calculateData(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val data = rawData
      .withColumn("is_step2", when(col("step2_percent")===1, 1).otherwise(0))
      .withColumn("cpa_ratio", col("cpa_given") * 1.0 / col("cpa_real"))
      .withColumn("is_high_cpa", when(col("is_step2")===1 && (col("cpa_ratio")<0.8 || col("cpa_real").isNull), 1).otherwise(0))
      .withColumn("is_low_cpa", when(col("is_step2")===1 && col("cpa_ratio")>=0.8, 1).otherwise(0))
      .withColumn("cost", col("price") * col("ctr_cnt"))
  }
}
