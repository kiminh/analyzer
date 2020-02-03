package com.cpc.spark.oCPX.cv_recall.deep_cv

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcDeepCVrecall_assessment {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hourInt = args(1).toInt
    val dbName = args(2).toString
    println("parameters:")
    println(s"date=$date")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcShallowCVrecall_predict: $date").enableHiveSupport().getOrCreate()

    val rawData1 = cvRecallAssessment(date, 1, spark)
    val rawData2 = cvRecallAssessment(date, 2, spark)
    val rawData3 = cvRecallAssessment(date, 3, spark)

    val rawData = rawData1.union(rawData2).union(rawData3)

  }

  def cvRecallAssessment(date: String, dateInt: Int, spark: SparkSession) = {
    val cvData = calculateCV(date, spark)

    val rawData = calculateCvValue(cvData, dateInt, spark)

    val recallValue1 = cvRecallPredictV1(date, dateInt, spark)
    val recallValue2 = cvRecallPredictV2(date, dateInt, spark)

    val result = rawData
      .join(recallValue1, Seq("deep_conversion_goal"), "left_outer")
      .join(recallValue2, Seq("userid", "deep_conversion_goal"), "left_outer")
      .na.fill(1.0, Seq("recall_value1"))
      .withColumn("recall_value", when(col("recall_value2").isNull, col("recall_value1")).otherwise(col("recall_value2")))
      .select("unitid", "userid", "deep_conversion_goal", "total_cv", "cv", "recall_value", "date_cnt", "recall_value1", "recall_value2")
      .withColumn("pred_cv", col("cv") * col("recall_value"))
      .withColumn("pred_cv1", col("cv") * col("recall_value1"))
      .withColumn("date_diff", lit(dateInt))

    result
  }

  def cvRecallPredictV2(date: String, dateInt: Int, spark: SparkSession) = {
    /*
    dl_cpc.algo_recall_info_v2
     */
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |   id as userid,
         |   conversion_goal as deep_conversion_goal,
         |   (case when hour_diff = 24 then 1
         |         when hour_diff = 48 then 2
         |         else 3
         |   end) as date_diff,
         |   avg(value) as recall_value2
         |FROM
         |  dl_cpc.algo_recall_info_v2
         |WHERE
         |  version = 'v_userid'
         |AND
         |  day = '$date1'
         |AND
         |  hour = '23'
         |GROUP BY
         |  id,
         |  conversion_goal,
         |  (case when hour_diff = 24 then 1
         |      when hour_diff = 48 then 2
         |      else 3
         |   end)
         |""".stripMargin
    println(sqlRequest)
    val recallValue = spark
      .sql(sqlRequest)
      .filter(s"date_diff = $dateInt")
      .select("userid", "deep_conversion_goal", "recall_value2")
    recallValue
  }

  def cvRecallPredictV1(date: String, dateInt: Int, spark: SparkSession) = {
    /*
    dl_cpc.algo_recall_info_v2
     */
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |   conversion_goal as deep_conversion_goal,
         |   (case when hour_diff = 24 then 1
         |         when hour_diff = 48 then 2
         |         else 3
         |   end) as date_diff,
         |   avg(value) as recall_value1
         |FROM
         |  dl_cpc.algo_recall_info_v2
         |WHERE
         |  version = 'v1'
         |AND
         |  day = '$date1'
         |AND
         |  hour = '23'
         |GROUP BY
         |  conversion_goal,
         |  (case when hour_diff = 24 then 1
         |      when hour_diff = 48 then 2
         |      else 3
         |   end)
         |""".stripMargin
    println(sqlRequest)
    val recallValue = spark
      .sql(sqlRequest)
      .filter(s"date_diff = $dateInt")
      .select("deep_conversion_goal", "recall_value1")
    recallValue
  }

  def calculateCvValue(data: DataFrame, dateInt: Int, spark: SparkSession) = {
    val totalCV = data
      .groupBy("unitid", "userid", "deep_conversion_goal")
      .agg(
        sum(col("cv")).alias("total_cv")
      )
      .select("unitid", "userid", "deep_conversion_goal", "total_cv")

    val clickCV = data
      .filter(s"cv_date_diff <= $dateInt")
      .groupBy("unitid", "userid", "deep_conversion_goal")
      .agg(
        sum(col("cv")).alias("cv")
      )
      .select("unitid", "userid", "deep_conversion_goal", "cv")

    val result = totalCV
      .join(clickCV, Seq("unitid", "userid", "deep_conversion_goal"), "inner")
      .select("unitid", "userid", "deep_conversion_goal", "total_cv", "cv")
      .withColumn("date_cnt", lit(dateInt))

    result
  }

  def calculateCV(date: String, spark: SparkSession) = {
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    deep_conversion_goal,
         |    price,
         |    date as click_date
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    `date` = '$date'
         |AND
         |    is_deep_ocpc = 1
         |AND
         |    isclick = 1
         |""".stripMargin
    println(sqlRequest1)
    val clickData = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |    searchid,
         |    deep_conversion_goal,
         |    date,
         |    1 as iscvr,
         |    row_number() over(partition by searchid, deep_conversion_goal order by date) as seq
         |FROM
         |    dl_cpc.ocpc_label_deep_cvr_hourly
         |WHERE
         |    date >= '$date'
         |""".stripMargin
    println(sqlRequest2)
    val cvData = spark
      .sql(sqlRequest2)
      .filter(s"seq = 1")
      .withColumn("cv_date", col("date"))
      .select("searchid", "deep_conversion_goal", "cv_date")

    val baseData = clickData
      .join(cvData, Seq("searchid", "deep_conversion_goal"), "inner")
      .select("searchid", "unitid", "userid", "deep_conversion_goal", "click_date", "cv_date")
      .withColumn("click_date_diff", udfCalculateDateDiff(date)(col("click_date")))
      .withColumn("cv_date_diff", udfCalculateDateDiff(date)(col("cv_date")))

    baseData.createOrReplaceTempView("base_data")

    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  deep_conversion_goal,
         |  click_date_diff,
         |  cv_date_diff,
         |  count(distinct searchid) as cv
         |FROM
         |  base_data
         |GROUP BY unitid, userid, deep_conversion_goal, click_date_diff, cv_date_diff
         |""".stripMargin
    println(sqlRequest3)
    val data = spark.sql(sqlRequest3).cache()

    data.show(10)

    data
  }

  def udfCalculateDateDiff(date: String) = udf((date1: String) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")

    val nowTime = dateConverter.parse(date1)
    val ocpcTime = dateConverter.parse(date)
    val dateDiff = (nowTime.getTime() - ocpcTime.getTime()) / (1000 * 60 * 60 * 24)

    dateDiff
  })


}