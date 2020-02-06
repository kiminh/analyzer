package com.cpc.spark.oCPX.cv_recall.shallow_cv

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import com.cpc.spark.oCPX.cv_recall.shallow_cv.OcpcShallowCVrecall_predict.cvRecallPredict
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcShallowCVrecall_assessmentV2 {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hourInt = args(1).toInt
    println("parameters:")
    println(s"date=$date")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcShallowCVrecall_predict: $date").enableHiveSupport().getOrCreate()

    val data = cvRecallAssessment(date, hourInt, spark)

    data
      .write.mode("overwrite").saveAsTable("test.check_shallow_recall_cv_ocpc_data20200206a")


  }

  def cvRecallAssessment(date: String, hourInt: Int, spark: SparkSession) = {
    val cvData = calculateCV(date, hourInt, spark)

    var realCvData = calculateCvValue(cvData, 1, hourInt, spark)

    for (startHour <- 2 to 24) {
        println(s"########  startHour = $startHour  #######")
      val singleData = calculateCvValue(cvData, startHour, hourInt, spark)
      realCvData = realCvData.union(singleData)
    }

    // todo
    // 预召回
    val recallValue1 = cvRecallPredictV1(date, spark)
    val recallValue2 = cvRecallPredictV2(date, spark)
    val predCvData = predictCvValue(cvData, 1, hourInt, recallValue1, recallValue2, spark)

    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -7)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val recallValueRaw = cvRecallPredict(date1, hourInt, spark)
    val recallValue = recallValueRaw
      .selectExpr("cast(userid as int) userid", "conversion_goal", "recall_value")

    val result = realCvData
        .join(recallValue, Seq("userid", "conversion_goal"), "left_outer")
        .na.fill(1.0, Seq("recall_value"))
        .select("unitid", "userid", "conversion_goal", "total_cv", "cost", "cv", "recall_value", "start_hour")
        .withColumn("pred_cv", col("cv") * col("recall_value"))

    result
  }

  def cvRecallPredictV1(date: String, spark: SparkSession) = {
    /*
    recall value by conversion_goal
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
         |  conversion_goal,
         |  date_click,
         |  hour_diff,
         |  recall_ratio
         |FROM
         |  dl_cpc.ocpc_cvr_pre_recall_ratio
         |WHERE
         |  date = '$date1'
         |AND
         |  userid = 'all'
         |AND
         |  recall_ratio is not null
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .filter(s"conversion_goal in (2, 5)")
      .groupBy("conversion_goal", "date_click", "hour_diff")
      .agg(
        avg(col("recall_ratio")).alias("recall_ratio")
      )
      .withColumn("date", col("date_click"))
      .withColumn("recall_value1", lit(1) * 1.0 / col("recall_ratio"))
      .filter(s"recall_value1 is not null")
      .select("conversion_goal", "date", "hour_diff", "recall_value1")

    data
  }

  def cvRecallPredictV2(date: String, spark: SparkSession) = {
    /*
    recall value by conversion_goal
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
         |  conversion_goal,
         |  cast(userid as int) as userid,
         |  date_click,
         |  hour_diff,
         |  recall_ratio
         |FROM
         |  dl_cpc.ocpc_cvr_pre_recall_ratio
         |WHERE
         |  date = '$date1'
         |AND
         |  userid != 'all'
         |AND
         |  recall_ratio is not null
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .filter(s"conversion_goal in (2, 5)")
      .groupBy("conversion_goal", "userid", "date_click", "hour_diff")
      .agg(
        avg(col("recall_ratio")).alias("recall_ratio")
      )
      .withColumn("date", col("date_click"))
      .withColumn("recall_value2", lit(1) * 1.0 / col("recall_ratio"))
      .filter(s"recall_value2 is not null")
      .select("conversion_goal", "userid", "date", "hour_diff", "recall_value2")
      .cache()

    data
  }

  def predictCvValue(baseData: DataFrame, startHour: Int, hourInt: Int, recallValue1: DataFrame, recallValue2: DataFrame, spark: SparkSession) = {
    // todo
    val endHour = startHour + hourInt
    val data = baseData
      .filter(s"click_hour_diff >= $startHour and click_hour_diff < $endHour")
      .withColumn("date", col("click_date"))

    val dataRaw = data
      .filter(s"cv_hour_diff >= $startHour and cv_hour_diff < $endHour")
      .withColumn("hour_diff", col("click_hour_diff") - lit(startHour))

    val joinData = dataRaw
      .join(recallValue1, Seq("conversion_goal", "date", "hour_diff"), "left_outer")
      .na.fill(1.0, Seq("recall_value1"))
      .join(recallValue2, Seq("userid", "conversion_goal", "date", "hour_diff"), "left_outer")
      .withColumn("recall_value", when(col("recall_value2").isNull, col("recall_value1")).otherwise(col("recall_value2")))
      .withColumn("cv_recall", col("cv") * col("recall_value"))
      .withColumn("pred_cv", when(col("cv_recall") > col("click"), col("click")).otherwise(col("cv_recall")))

    joinData
  }

  def calculateCvValue(baseData: DataFrame, startHour: Int, hourInt: Int, spark: SparkSession) = {
    val endHour = startHour + hourInt
    val data = baseData.filter(s"click_hour_diff >= $startHour and click_hour_diff < $endHour")

    val totalCV = data
      .groupBy("unitid", "userid", "conversion_goal")
      .agg(
        sum(col("cv")).alias("total_cv"),
        sum(col("cost")).alias("cost")
      )
      .select("unitid", "userid", "conversion_goal", "total_cv", "cost")

    val clickCV = data
      .filter(s"cv_hour_diff >= $startHour and cv_hour_diff < $endHour")
      .groupBy("unitid", "userid", "conversion_goal")
      .agg(sum(col("cv")).alias("cv"))
      .select("unitid", "userid", "conversion_goal", "cv")

    val result = totalCV
      .join(clickCV, Seq("unitid", "userid", "conversion_goal"), "inner")
      .select("unitid", "userid", "conversion_goal", "total_cv", "cost", "cv")
      .withColumn("start_hour", lit(startHour))

    result
  }

  def calculateCV(date: String, hourInt: Int, spark: SparkSession) = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + "23"
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date, "00", date1, hour1)

    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    conversion_goal,
         |    conversion_from,
         |    date as click_date,
         |    hour as click_hour
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    $selectCondition
         |AND
         |    is_ocpc = 1
         |AND
         |    conversion_goal in (2, 5)
         |AND
         |    isclick = 1
         |""".stripMargin
    println(sqlRequest1)
    val clickData = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |    searchid,
         |    conversion_goal,
         |    conversion_from,
         |    date,
         |    hour,
         |    1 as iscvr,
         |    row_number() over(partition by searchid, conversion_goal, conversion_from order by date, hour) as seq
         |FROM
         |    dl_cpc.ocpc_cvr_log_hourly
         |WHERE
         |    date >= '$date'
         |""".stripMargin
    println(sqlRequest2)
    val cvData = spark
      .sql(sqlRequest2)
      .filter(s"seq = 1")
      .withColumn("cv_date", col("date"))
      .withColumn("cv_hour", col("hour"))
      .select("searchid", "conversion_goal", "conversion_from", "cv_date", "cv_hour")

    val baseData = clickData
      .join(cvData, Seq("searchid", "conversion_goal", "conversion_from"), "inner")
      .select("searchid", "unitid", "userid", "conversion_goal", "conversion_from", "click_date", "click_hour", "cv_date", "cv_hour")
      .withColumn("click_hour_diff", udfCalculateHourDiff(date, "00")(col("click_date"), col("click_hour")))
      .withColumn("cv_hour_diff", udfCalculateHourDiff(date, "00")(col("cv_date"), col("cv_hour")))

    baseData.createOrReplaceTempView("base_data")

    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  click_hour_diff,
         |  cv_hour_diff,
         |  click_date,
         |  count(distinct searchid) as cv
         |FROM
         |  base_data
         |GROUP BY unitid, userid, conversion_goal, click_hour_diff, cv_hour_diff, click_date
         |""".stripMargin
    println(sqlRequest3)
    val data = spark.sql(sqlRequest3).cache()

    data.show(10)

    data
  }

  def udfCalculateHourDiff(date: String, hour: String) = udf((date1: String, hour1: String) => {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")

    val nowTime = dateConverter.parse(date1 + " " + hour1)
    val ocpcTime = dateConverter.parse(date + " " + hour)
    val hourDiff = (nowTime.getTime() - ocpcTime.getTime()) / (1000 * 60 * 60)

    hourDiff
  })


}