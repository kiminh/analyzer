package com.cpc.spark.ocpcV3.ocpcQtt.warning

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcConversionWarning {
  def main(args: Array[String]): Unit = {
    /*
    ocpc的api回传转化类数据监控：
    1. 当日回传转化数过少
    2. 单日回传转化中大量转化无法被有效关联
     */
    val date = args(0).toString

    val spark = SparkSession.builder().appName(s"ocpc QTT cvr warning: $date").enableHiveSupport().getOrCreate()

    // 比较数据
    val cmp1 = checkCvrNum(date, spark)
    val cmp2 = checkCvrUseful(date, spark)

    cmp1.write.mode("overwrite").saveAsTable("test.ocpc_warning_cvr_num_daily")
    cmp2.write.mode("overwrite").saveAsTable("test.ocpc_warning_cvr_percent_daily")


  }

  def getCtrData(date: String, dayCnt: Int, spark: SparkSession) = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -dayCnt)
    val start_date = calendar.getTime
    val date1 = sdf.format(start_date)

    val selectCondition1 = s"`dt` = '$date1'"
    val selectCondition2 = s"`date` = '$date1'"

    val sqlRequest =
      s"""
         |SELECT
         |  a.searchid,
         |  a.ideaid,
         |  a.price,
         |  a.ocpc_log_dict,
         |  a.isclick,
         |  b.iscvr
         |FROM
         |  (SELECT
         |    *
         |   FROM
         |    dl_cpc.ocpc_unionlog
         |   WHERE
         |    $selectCondition1
         |   AND
         |    isclick=1) as a
         |LEFT JOIN
         |  (SELECT
         |    searchid,
         |    label as iscvr
         |   FROM
           |    dl_cpc.ml_cvr_feature_v2
         |   WHERE
         |    $selectCondition2
         |   AND
         |    label=1
         |   GROUP BY searchid, label) as b
         |ON
         |  a.searchid=b.searchid
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val result = data
      .groupBy("ideaid")
      .agg(
        sum(col("price")).alias("cost"),
        sum(col("isclick")).alias("ctrcnt"),
        sum(col("iscvr")).alias("cvrcnt"))
      .withColumn("cvr", col("cvrcnt") * 1.0 / col("ctrcnt"))
      .withColumn("cpa", col("cost") * 1.0 / col("cvrcnt"))
      .withColumn("date", lit(date1))
    result
  }

  def getCvrData(date: String, dayCnt: Int, spark: SparkSession) = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -dayCnt)
    val start_date = calendar.getTime
    val date1 = sdf.format(start_date)

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  searchid,
         |  label as iscvr
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  `date` = '$date1'
         |AND
         |  label=1
         |GROUP BY ideaid, searchid, label
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val result = data
      .groupBy("ideaid")
      .agg(sum(col("iscvr")).alias("cvrcnt_total"))
      .withColumn("date", lit(date1))

    result
  }

  def checkCvrNum(date: String, spark: SparkSession) = {
    // 当天数据
    val currentData = getCtrData(date, 0, spark)

    // 历史前三天数据
    val historyData1 = getCtrData(date, 1, spark)
    val historyData2 = getCtrData(date, 2, spark)
    val historyData3 = getCtrData(date, 3, spark)

    val historyData = historyData1
      .union(historyData2)
      .union(historyData3)
      .withColumn("day", lit(1))

    historyData.show(10)

    val finalHistoryData = historyData
      .groupBy("ideaid")
      .agg(
        sum(col("day")).alias("day_cnt"),
        sum(col("cost")).alias("cost"),
        sum(col("ctrcnt")).alias("ctrcnt"),
        sum(col("cvrcnt")).alias("cvrcnt")
      )
      .withColumn("history_cvr", col("cvrcnt") * 1.0 / col("ctrcnt"))
      .withColumn("history_cpa", col("cost") * 1.0 / col("cvrcnt"))
      .withColumn("history_cost", col("cost") * 1.0 / col("day_cnt"))
      .withColumn("history_ctrcnt", col("ctrcnt") * 1.0 / col("day_cnt"))
      .withColumn("history_cvrcnt", col("cvrcnt") * 1.0 / col("day_cnt"))
      .select("ideaid", "history_cost", "history_ctrcnt", "history_cvrcnt", "history_cvr", "history_cpa")

    // 比较当日数据与历史数据
    val resultDF = currentData
      .join(historyData, Seq("ideaid"), "inner")
      .select("ideaid", "cost", "ctrcnt", "cvrcnt", "cvr", "cpa", "history_cost", "history_ctrcnt", "history_cvrcnt", "history_cvr", "history_cpa")
      .withColumn("cost_percent", abs(col("cost") - col("history_cost")) * 1.0 / col("history_cost"))
      .withColumn("ctrcnt_percent", abs(col("ctrcnt") - col("history_ctrcnt")) * 1.0 / col("history_ctrcnt"))
      .withColumn("cvrcnt_percent", abs(col("cvrcnt") - col("history_cvrcnt")) * 1.0 / col("history_cvrcnt"))
      .withColumn("cvr_percent", abs(col("cvr") - col("history_cvr")) * 1.0 / col("history_cvr"))
      .withColumn("cpa_percent", abs(col("cpa") - col("history_cpa")) * 1.0 / col("history_cpa"))
      .withColumn("date", lit(date))


    resultDF
  }

  def checkCvrUseful(date: String, spark: SparkSession) = {
    // 当天数据
    val currentCtrData = getCtrData(date, 0, spark)
    val currentCvrData = getCvrData(date, 0, spark)
    val currentData = currentCtrData
      .join(currentCvrData, Seq("ideaid"), "outer")
      .select("ideaid", "cvrcnt", "cvrcnt_total")
      .na.fill(0, Seq("cvrcnt", "cvrcnt_total"))

    // 历史前三天数据
    val historyCtrData1 = getCtrData(date, 1, spark)
    val historyCvrData1 = getCvrData(date, 1, spark)
    val historyData1 = historyCtrData1
      .join(historyCvrData1, Seq("ideaid"), "outer")
      .select("ideaid", "cvrcnt", "cvrcnt_total")

    val historyCtrData2 = getCtrData(date, 2, spark)
    val historyCvrData2 = getCvrData(date, 2, spark)
    val historyData2 = historyCtrData2
      .join(historyCvrData2, Seq("ideaid"), "outer")
      .select("ideaid", "cvrcnt", "cvrcnt_total")

    val historyCtrData3 = getCtrData(date, 3, spark)
    val historyCvrData3 = getCvrData(date, 3, spark)
    val historyData3 = historyCtrData3
      .join(historyCvrData3, Seq("ideaid"), "outer")
      .select("ideaid", "cvrcnt", "cvrcnt_total")

    val historyFinalData = historyData1
      .union(historyData2)
      .union(historyData3)
      .withColumn("day", lit(1))
      .groupBy("ideaid")
      .agg(
        sum(col("cvrcnt")).alias("cvrcnt"),
        sum(col("cvrcnt_total")).alias("cvrcnt_total"),
        sum(col("day")).alias("day_cnt")
      )
      .withColumn("history_cvrcnt", col("cvrcnt") * 1.0 / col("day_cnt"))
      .withColumn("history_cvrcnt_total", col("cvrcnt_total") * 1.0 / col("day_cnt"))
      .select("ideaid", "history_cvrcnt", "history_cvrcnt_total")
      .na.fill(0, Seq("history_cvrcnt", "history_cvrcnt_total"))

    // 数据比对
    val resultDF = currentData
      .join(historyFinalData, Seq("ideaid"), "inner")
      .withColumn("current_cvr_percent", col("cvrcnt") * 1.0 / col("cvrcnt_total"))
      .withColumn("history_cvr_percent", col("history_cvrcnt") * 1.0 / col("history_cvrcnt_total"))
      .withColumn("cvrcnt_percent", abs(col("current_cvr_percent") - col("history_cvr_percent")) * 1.0 / col("history_cvr_percent"))
      .withColumn("date", lit(date))

    resultDF
  }

}