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

    getResult(date, spark)
  }

  def getResult(date: String, spark: SparkSession) = {
    import spark.implicits._

    val filename = "/user/cpc/wangjun/ocpc_exp_ideas.txt"
    val data = spark.sparkContext.textFile(filename)
    val rawRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    rawRDD.foreach(println)
    val rawDF = rawRDD.toDF("ideaid", "flag").distinct()
    rawDF.write.mode("overwrite").saveAsTable("test.ocpc_regression_k_idea_list")

    val noApiTable = spark
      .table("dl_cpc.ocpc_check_daily_report_noapi")
      .where(s"`date`='$date'")

    val apiTable = spark
      .table("dl_cpc.ocpc_check_daily_report_api")
      .where(s"`date`='$date'")

    val noApiResult = noApiTable
      .join(rawDF, Seq("ideaid"), "left_outer")
      .withColumn("k_model", when(col("flag")===1, "regression").otherwise("no_regression"))
    noApiResult.write.mode("overwrite").saveAsTable("test.ocpc_daily_report_noapi")

    val apiResult = apiTable
      .join(rawDF, Seq("ideaid"), "left_outer")
      .withColumn("k_model", when(col("flag")===1, "regression").otherwise("no_regression"))
    apiResult.write.mode("overwrite").saveAsTable("test.ocpc_daily_report_api")
  }

}