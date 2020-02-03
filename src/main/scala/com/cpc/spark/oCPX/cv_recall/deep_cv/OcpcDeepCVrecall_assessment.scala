package com.cpc.spark.oCPX.cv_recall.deep_cv

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import com.cpc.spark.oCPX.cv_recall.shallow_cv.OcpcShallowCVrecall_predict.cvRecallPredict
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

    val data = cvRecallAssessment(date, spark)


  }

  def cvRecallAssessment(date: String, spark: SparkSession) = {
    val cvData = calculateCV(date, spark)

    val rawData = calculateCvValue(cvData, 1, spark)

    val recallValue = cvRecallPredict(date, 1, "v1", spark)

    val result = rawData
      .join(recallValue, Seq("deep_conversion_goal"), "left_outer")
      .na.fill(1.0, Seq("recall_value"))
      .select("unitid", "userid", "deep_conversion_goal", "total_cv", "cv", "recall_value", "date_cnt")
      .withColumn("pred_cv", col("cv") * col("recall_value"))

    result
  }

  def cvRecallPredict(date: String, dateInt: Int, version: String, spark: SparkSession) = {
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
         |   avg(value) as recall_value
         |FROM
         |  dl_cpc.algo_recall_info_v2
         |WHERE
         |  version = '$version'
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