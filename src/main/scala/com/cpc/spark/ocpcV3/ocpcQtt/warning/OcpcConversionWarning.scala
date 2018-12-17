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
//    cmp1.write.mode("overwrite").insertInto("dl_cpc.ocpc_warning_cvr_num_daily")
//    cmp2.write.mode("overwrite").insertInto("dl_cpc.ocpc_warning_cvr_percent_daily")


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
         |  b.iscvr1,
         |  c.iscvr2
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
         |    label2 as iscvr1
         |   FROM
         |    dl_cpc.ml_cvr_feature_v1
         |   WHERE
         |    $selectCondition2
         |   AND
         |    label2=1
         |   GROUP BY searchid, label2) as b
         |ON
         |    a.searchid=b.searchid
         |LEFT JOIN
         |  (SELECT
         |    searchid,
         |    label as iscvr2
         |   FROM
         |    dl_cpc.ml_cvr_feature_v2
         |   WHERE
         |    $selectCondition2
         |   AND
         |    label=1
         |   GROUP BY searchid, label) as c
         |ON
         |  a.searchid=c.searchid
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    val result = data
      .groupBy("ideaid")
      .agg(
        sum(col("price")).alias("cost"),
        sum(col("isclick")).alias("ctrcnt"),
        sum(col("iscvr1")).alias("cvr1cnt"),
        sum(col("iscvr2")).alias("cvr2cnt"))
      .withColumn("cvr1", col("cvr1cnt") * 1.0 / col("ctrcnt"))
      .withColumn("cvr2", col("cvr2cnt") * 1.0 / col("ctrcnt"))
      .withColumn("cpa1", col("cost") * 1.0 / col("cvr1cnt"))
      .withColumn("cpa2", col("cost") * 1.0 / col("cvr2cnt"))
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

    val sqlRequest1 =
      s"""
         |SELECT
         |  ideaid,
         |  searchid,
         |  label2 as iscvr1
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  `date` = '$date1'
         |AND
         |  label2=1
         |GROUP BY ideaid, searchid, label2
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |  ideaid,
         |  searchid,
         |  label as iscvr2
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  `date` = '$date1'
         |AND
         |  label=1
         |GROUP BY ideaid, searchid, label
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)

    val result1 = data1
      .groupBy("ideaid")
      .agg(sum(col("iscvr1")).alias("cvr1cnt_total"))

    val result2 = data2
      .groupBy("ideaid")
      .agg(sum(col("iscvr2")).alias("cvr2cnt_total"))

    val result = result1
      .join(result2, Seq("ideaid"), "outer")
      .withColumn("date", lit(date))

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
        sum(col("cvr1cnt")).alias("cvr1cnt"),
        sum(col("cvr2cnt")).alias("cvr2cnt")
      )
      .withColumn("history_cvr1", col("cvr1cnt") * 1.0 / col("ctrcnt"))
      .withColumn("history_cvr2", col("cvr2cnt") * 1.0 / col("ctrcnt"))
      .withColumn("history_cpa1", col("cost") * 1.0 / col("cvr1cnt"))
      .withColumn("history_cpa2", col("cost") * 1.0 / col("cvr2cnt"))
      .withColumn("history_cost", col("cost") * 1.0 / col("day_cnt"))
      .withColumn("history_ctrcnt", col("ctrcnt") * 1.0 / col("day_cnt"))
      .withColumn("history_cvr1cnt", col("cvr1cnt") * 1.0 / col("day_cnt"))
      .withColumn("history_cvr2cnt", col("cvr2cnt") * 1.0 / col("day_cnt"))
      .select("ideaid", "history_cost", "history_ctrcnt", "history_cvr1cnt", "history_cvr2cnt", "history_cvr1", "history_cvr2", "history_cpa1", "history_cpa2")

    // 比较当日数据与历史数据
    currentData.printSchema()
    finalHistoryData.printSchema()
    val resultDF = currentData
      .join(finalHistoryData, Seq("ideaid"), "inner")
      .select("ideaid", "cost", "ctrcnt", "cvr1cnt", "cvr2cnt", "cvr1", "cvr2", "cpa1", "cpa2", "history_cost", "history_ctrcnt", "history_cvr1cnt", "history_cvr2cnt", "history_cvr1", "history_cvr2", "history_cpa1", "history_cpa2")
      .withColumn("cost_percent", abs(col("cost") - col("history_cost")) * 1.0 / col("history_cost"))
      .withColumn("ctrcnt_percent", abs(col("ctrcnt") - col("history_ctrcnt")) * 1.0 / col("history_ctrcnt"))
      .withColumn("cvr1cnt_percent", abs(col("cvr1cnt") - col("history_cvr1cnt")) * 1.0 / col("history_cvr1cnt"))
      .withColumn("cvr2cnt_percent", abs(col("cvr2cnt") - col("history_cvr2cnt")) * 1.0 / col("history_cvr2cnt"))
      .withColumn("cvr1_percent", abs(col("cvr1") - col("history_cvr1")) * 1.0 / col("history_cvr1"))
      .withColumn("cvr2_percent", abs(col("cvr2") - col("history_cvr2")) * 1.0 / col("history_cvr2"))
      .withColumn("cpa1_percent", abs(col("cpa1") - col("history_cpa1")) * 1.0 / col("history_cpa1"))
      .withColumn("cpa2_percent", abs(col("cpa2") - col("history_cpa2")) * 1.0 / col("history_cpa2"))
      .withColumn("date", lit(date))


    resultDF
  }

  def checkCvrUseful(date: String, spark: SparkSession) = {
    // 当天数据
    val currentCtrData = getCtrData(date, 0, spark)
    val currentCvrData = getCvrData(date, 0, spark)
    val currentData = currentCtrData
      .join(currentCvrData, Seq("ideaid"), "outer")
      .select("ideaid", "cvr1cnt", "cvr1cnt_total", "cvr2cnt", "cvr2cnt_total")
      .na.fill(0, Seq("cvr1cnt", "cvr1cnt_total", "cvr2cnt", "cvr2cnt_total"))

    // 历史前三天数据
    val historyCtrData1 = getCtrData(date, 1, spark)
    val historyCvrData1 = getCvrData(date, 1, spark)
    val historyData1 = historyCtrData1
      .join(historyCvrData1, Seq("ideaid"), "outer")
      .select("ideaid", "cvr1cnt_total", "cvr2cnt", "cvr2cnt_total")

    val historyCtrData2 = getCtrData(date, 2, spark)
    val historyCvrData2 = getCvrData(date, 2, spark)
    val historyData2 = historyCtrData2
      .join(historyCvrData2, Seq("ideaid"), "outer")
      .select("ideaid", "cvr1cnt_total", "cvr2cnt", "cvr2cnt_total")

    val historyCtrData3 = getCtrData(date, 3, spark)
    val historyCvrData3 = getCvrData(date, 3, spark)
    val historyData3 = historyCtrData3
      .join(historyCvrData3, Seq("ideaid"), "outer")
      .select("ideaid", "cvr1cnt_total", "cvr2cnt", "cvr2cnt_total")

    val historyFinalData = historyData1
      .union(historyData2)
      .union(historyData3)
      .withColumn("day", lit(1))
      .groupBy("ideaid")
      .agg(
        sum(col("cvr1cnt")).alias("cvr1cnt"),
        sum(col("cvr1cnt_total")).alias("cvr1cnt_total"),
        sum(col("cvr2cnt")).alias("cvr2cnt"),
        sum(col("cvr2cnt_total")).alias("cvr2cnt_total"),
        sum(col("day")).alias("day_cnt")
      )
      .withColumn("history_cvr1cnt", col("cvr1cnt") * 1.0 / col("day_cnt"))
      .withColumn("history_cvr2cnt", col("cvr2cnt") * 1.0 / col("day_cnt"))
      .withColumn("history_cvr1cnt_total", col("cvr1cnt_total") * 1.0 / col("day_cnt"))
      .withColumn("history_cvr2cnt_total", col("cvr2cnt_total") * 1.0 / col("day_cnt"))
      .select("ideaid", "history_cvr1cnt", "history_cvr1cnt_total", "history_cvr2cnt", "history_cvr2cnt_total")
      .na.fill(0, Seq("history_cvr1cnt", "history_cvr1cnt_total", "history_cvr2cnt", "history_cvr2cnt_total"))

    // 数据比对
    val resultDF = currentData
      .join(historyFinalData, Seq("ideaid"), "inner")
      .withColumn("current_cvr1_percent", col("cvr1cnt") * 1.0 / col("cvr1cnt_total"))
      .withColumn("current_cvr2_percent", col("cvr2cnt") * 1.0 / col("cvr2cnt_total"))
      .withColumn("history_cvr1_percent", col("history_cvr1cnt") * 1.0 / col("history_cvr1cnt_total"))
      .withColumn("history_cvr2_percent", col("history_cvr2cnt") * 1.0 / col("history_cvr2cnt_total"))
      .withColumn("cvr1cnt_percent", abs(col("current_cvr1_percent") - col("history_cvr1_percent")) * 1.0 / col("history_cvr1_percent"))
      .withColumn("cvr2cnt_percent", abs(col("current_cvr2_percent") - col("history_cvr2_percent")) * 1.0 / col("history_cvr2_percent"))
      .withColumn("date", lit(date))

    resultDF
  }

}