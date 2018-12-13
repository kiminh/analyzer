package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcCheckResultWithList {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("OcpcDailyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString

    val result = getResult(date, spark)
    val tableName = "test.ocpc_check_exp_result20181213"
    result.write.mode("overwrite").saveAsTable(tableName)
    println(s"successfully save data into table: $tableName")
  }

  def getResult(date: String, spark: SparkSession) = {
    import spark.implicits._

    val filename = "/user/cpc/wangjun/ocpc_exp_ideas.txt"
    val data = spark.sparkContext.textFile(filename)
    val rawRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    rawRDD.foreach(println)
    val rawDF = rawRDD.toDF("ideaid", "flag").distinct()
    rawDF.write.mode("overwrite").saveAsTable("test.ocpc_exp_ideaid_list")

    val rawData = spark
      .table("dl_cpc.ocpc_detail_report_hourly")
      .where(s"`date`='$date' and `hour`='23'")
      .withColumn("ideaid", col("idea_id"))

    val resultDF = rawData
      .join(rawDF, Seq("ideaid"), "left_outer")
      .withColumn("flag", when(col("flag").isNull, 0).otherwise(col("flag")))
      .select("user_id", "idea_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "flag")

    resultDF
  }

}