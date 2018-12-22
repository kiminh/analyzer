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
    val hour = args(1).toString

    val result = getDetailResult(date, hour, spark)
    val tableName = "test.ocpc_check_exp_result20181222"
    result.write.mode("overwrite").saveAsTable(tableName)
    println(s"successfully save data into table: $tableName")
  }

  def getResult(date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._

    val filename = "/user/cpc/wangjun/ocpc_exp_ideas.txt"
    val data = spark.sparkContext.textFile(filename)
    val rawRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    rawRDD.foreach(println)
    val rawDF = rawRDD.toDF("ideaid", "flag").distinct()
    rawDF.write.mode("overwrite").saveAsTable("test.ocpc_exp_ideaid_list")

    val rawData = spark
      .table("dl_cpc.ocpc_detail_report_hourly")
      .where(s"`date`='$date' and `hour`='$hour'")
      .withColumn("ideaid", col("idea_id"))

    val resultDF = rawData
      .join(rawDF, Seq("ideaid"), "left_outer")
      .withColumn("flag", when(col("flag").isNull, 0).otherwise(col("flag")))
      .select("user_id", "idea_id", "conversion_goal", "step2_click_percent", "is_step2", "cpa_given", "cpa_real", "cpa_ratio", "is_cpa_ok", "impression", "click", "conversion", "ctr", "click_cvr", "show_cvr", "cost", "acp", "avg_k", "recent_k", "flag")

    resultDF
  }

  def getDetailResult(date: String, hour: String, spark: SparkSession) = {
    import spark.implicits._

    val filename = "/user/cpc/wangjun/ocpc_exp_ideas.txt"
    val data = spark.sparkContext.textFile(filename)
    val rawRDD = data.map(x => (x.split(",")(0).toInt, x.split(",")(1).toInt))
    rawRDD.foreach(println)
    val expDF = rawRDD.toDF("ideaid", "flag").filter(s"flag=2").distinct()
    expDF.write.mode("overwrite").saveAsTable("test.ocpc_exp_ideaid_list")

    val ctrData = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(s"`dt`='$date'")
      .withColumn("ideaid", col("idea_id"))
      .select("searchid", "ideaid", "ocpc_log_dict", "exp_ctr", "exp_cvr", "price", "isclick", "isshow", "hour")

    val cvrData = spark
      .table("dl_cpc.ml_cvr_feature_v1")
      .where(s"`date`='$date'")
      .select("searchid", "label_sdk_dlapp")
      .withColumn("iscvr", col("label_sdk_dlapp"))
      .select("searchid", "iscvr")
      .distinct()

    val rawData = ctrData.join(cvrData, Seq("searchid"), "left_outer")

    val resultDF = rawData
      .join(expDF, Seq("ideaid"), "inner")
      .select("searchid", "ideaid", "ocpc_log_dict", "exp_ctr", "exp_cvr", "price", "isclick", "isshow", "iscvr", "hour")

    resultDF
  }


}