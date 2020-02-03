package com.cpc.spark.oCPX.cv_recall.deep_cv

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, mapMediaName}
import com.cpc.spark.oCPX.cv_recall.deep_cv.OcpcDeepCVrecall_assessment.cvRecallAssessment
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcDeepCVrecall_predict_v1 {
  /*
  采用激活次留率和激活数来预估近48小时内的次留数
  激活次留率采用近3天同账户的激活次留率
  激活数采用同单元在同媒体流量下的数据
   */
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hourInt = args(1).toInt
    val dbName = args(2).toString
    println("parameters:")
    println(s"date=$date")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcShallowCVrecall_predict: $date").enableHiveSupport().getOrCreate()

//    val result =


  }


  def assessResult(date: String, spark: SparkSession) = {
    val dataRaw1 = cvRecallAssessment(date, 1, spark)
    val dataRaw2 = cvRecallPredict(date, spark)

    val data1 = dataRaw1
      .filter(s"deep_conversion_goal = 2")
      .select("unitid", "userid", "deep_conversion_goal", "total_cv", "cv", "recall_value", "date_cnt", "recall_value1", "recall_value2", "pred_cv", "pred_cv1")

    val data2 = dataRaw2
      .withColumn("pred_cv2", col("pred_cv"))
      .select("unitid", "userid", "pred_cv2", "deep_cvr")

    val data = data1
      .join(data2, Seq("unitid", "userid"), "left_outer")

    data
  }


  def cvRecallPredict(date: String, spark: SparkSession) = {
    val deepCvr = calculateDeepCvr(date, 3, spark)

    val cvData = getShallowCV(date, spark)
    val data = calculateCvValue(cvData, deepCvr, spark)


    data
  }

  def calculateCvValue(data: DataFrame, deepCvr: DataFrame, spark: SparkSession) = {
    val cvData = data
      .filter(s"cv_date_diff <= 1")
      .groupBy("unitid", "userid")
      .agg(
        sum(col("cv")).alias("cv")
      )
      .select("unitid", "userid", "cv")

    val result = cvData
        .join(deepCvr, Seq("userid"), "left_outer")
        .na.fill(1.0, Seq("deep_cvr"))
        .withColumn("pred_cv", col("cv") * col("deep_cvr"))

    result
  }

  def getShallowCV(date: String, spark: SparkSession) = {
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    date as click_date
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    `date` = '$date'
         |AND
         |    is_deep_ocpc = 1
         |AND
         |    isclick = 1
         |AND
         |    deep_conversion_goal = 2
         |""".stripMargin
    println(sqlRequest1)
    val clickData = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |    searchid,
         |    date,
         |    1 as iscvr,
         |    row_number() over(partition by searchid, deep_conversion_goal order by date) as seq
         |FROM
         |    dl_cpc.ocpc_cvr_log_hourly
         |WHERE
         |    date >= '$date'
         |AND
         |    conversion_goal = 2
         |""".stripMargin
    println(sqlRequest2)
    val cvData = spark
      .sql(sqlRequest2)
      .filter(s"seq = 1")
      .withColumn("cv_date", col("date"))
      .select("searchid", "cv_date")

    val baseData = clickData
      .join(cvData, Seq("searchid"), "inner")
      .select("searchid", "unitid", "userid", "click_date", "cv_date")
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

  def calculateDeepCvr(date: String, dayInt: Int, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val date1String = calendar.getTime
    val date1 = dateConverter.format(date1String)
    calendar.add(Calendar.DATE, -1)
    val date2String = calendar.getTime
    val date2 = dateConverter.format(date2String)
    calendar.add(Calendar.DATE, -dayInt)
    val date3String = calendar.getTime
    val date3 = dateConverter.format(date3String)

    // 激活数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  userid,
         |  conversion_goal,
         |  1 as iscvr1
         |FROM
         |  dl_cpc.cpc_conversion
         |WHERE
         |  day between '$date3' and '$date2'
         |AND
         |  array_contains(conversion_target, 'api_app_active')
         |""".stripMargin
    println(sqlRequest1)
    val data1Raw = spark
      .sql(sqlRequest1)
      .distinct()

    val data1 = mapMediaName(data1Raw, spark)

    // 次留数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr2
         |FROM
         |  dl_cpc.cpc_conversion
         |WHERE
         |  day >= '$date3'
         |AND
         |  array_contains(conversion_target, 'api_app_retention')
         |""".stripMargin
    println(sqlRequest2)
    val data2 = spark
      .sql(sqlRequest2)
      .distinct()

    val data = data1
      .join(data2, Seq("searchid"), "left_outer")
      .groupBy("userid")
      .agg(
        sum(col("iscvr1")).alias("cv1"),
        sum(col("iscvr2")).alias("cv2")
      )
      .select("userid", "cv1", "cv2")
      .filter(s"cv2 >= 10")
      .withColumn("deep_cvr", col("cv2") * 1.0 / col("cv1"))

    val resultDF = data.selectExpr("userid", "cast(deep_cvr as double) as deep_cvr")

    resultDF
  }


}