package com.cpc.spark.oCPX.cv_recall.shallow_cv

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.getTimeRangeSqlDate
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcShallowCVrecall_assessment {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hourInt = args(1).toInt
    val dbName = args(2).toString
    println("parameters:")
    println(s"date=$date")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcShallowCVrecall_predict: $date").enableHiveSupport().getOrCreate()

    val data = cvRecallPredict(date, hourInt, spark)

    val tableName = s"$dbName.ocpc_recall_value_daily"

    data
      .withColumn("id", col("userid"))
      .selectExpr("cast(id as string) id", "conversion_goal", "recall_value")
      .withColumn("date", lit(date))
      .withColumn("strat", lit("min_value"))
      .withColumn("hour_diff", lit(hourInt))
      .repartition(1)
      .write.mode("overwrite").insertInto(tableName)

  }

  def cvRecallPredict(date: String, hourInt: Int, spark: SparkSession) = {
    val cvData = calculateCV(date, hourInt, spark)

    var data = calculateRecallValue(cvData, 1, hourInt, spark)

    for (startHour <- 2 to 24) {
        println(s"########  startHour = $startHour  #######")
      val singleData = calculateRecallValue(cvData, startHour, hourInt, spark)
      data = data.union(singleData)
    }

    val result = data
        .groupBy("userid", "conversion_goal")
        .agg(
          avg(col("recall_value")).alias("recall_value")
        )
        .select("userid", "conversion_goal", "recall_value")
        .withColumn("recall_value", when(col("recall_value") < 1.0, 1.0).otherwise(when(col("recall_value") > 2.0, 2.0).otherwise(col("recall_value"))))
        .cache()

    result.show(10)
    result
  }

  def calculateRecallValue(baseData: DataFrame, startHour: Int, hourInt: Int, spark: SparkSession) = {
    val endHour = startHour + hourInt
    val data = baseData.filter(s"click_hour_diff >= $startHour and click_hour_diff < $endHour")

    val totalCV = data
      .groupBy("unitid", "userid", "conversion_goal")
      .agg(sum(col("cv")).alias("total_cv"))
      .select("unitid", "userid", "conversion_goal", "total_cv")

    val clickCV = data
      .filter(s"cv_hour_diff >= $startHour and cv_hour_diff < $endHour")
      .groupBy("unitid", "userid", "conversion_goal")
      .agg(sum(col("cv")).alias("cv"))
      .select("unitid", "userid", "conversion_goal", "cv")

    val result = totalCV
      .join(clickCV, Seq("unitid", "userid", "conversion_goal"), "inner")
      .select("unitid", "userid", "conversion_goal", "total_cv", "cv")
      .withColumn("recall_value", col("total_cv") * 1.0 / col("cv"))
      .filter(s"cv >= 80")

    val finalResult = result
      .groupBy("userid", "conversion_goal")
      .agg(
        min(col("recall_value")).alias("recall_value"),
        count(col("unitid")).alias("unit_cnt")
      )
      .select("userid", "conversion_goal", "recall_value", "unit_cnt")
      .filter(s"unit_cnt > 1")
      .withColumn("start_hour", lit(startHour))

    finalResult
  }

  def calculateCV(date: String, hourInt: Int, spark: SparkSession) = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + "00"
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, "23")

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
         |    date >= '$date1'
         |""".stripMargin
    println(sqlRequest2)
    val cvData1 = spark
      .sql(sqlRequest2)
      .filter(s"seq = 1")
      .withColumn("cv_date1", col("date"))
      .withColumn("cv_hour1", col("hour"))
      .select("searchid", "conversion_goal", "conversion_from", "cv_date1", "cv_hour1")

    val sqlRequest3 =
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
         |    $selectCondition
         |""".stripMargin
    println(sqlRequest3)
    val cvData2 = spark
      .sql(sqlRequest3)
      .filter(s"seq = 1")
      .withColumn("cv_date2", col("date"))
      .withColumn("cv_hour2", col("hour"))
      .select("searchid", "conversion_goal", "conversion_from", "cv_date2", "cv_hour2")

    val baseData = clickData
      .join(cvData1, Seq("searchid", "conversion_goal", "conversion_from"), "inner")
      .join(cvData2, Seq("searchid", "conversion_goal", "conversion_from"), "inner")
      .select("searchid", "unitid", "userid", "conversion_goal", "conversion_from", "click_date", "click_hour", "cv_date1", "cv_hour1", "cv_date2", "cv_hour2")
      .withColumn("click_hour_diff", udfCalculateHourDiff(date1, hour1)(col("click_date"), col("click_hour")))
      .withColumn("cv_hour_diff1", udfCalculateHourDiff(date1, hour1)(col("cv_date1"), col("cv_hour1")))
      .withColumn("cv_hour_diff2", udfCalculateHourDiff(date1, hour1)(col("cv_date2"), col("cv_hour2")))

    baseData.createOrReplaceTempView("base_data")

    val sqlRequest4 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  click_hour_diff,
         |  cv_hour_diff,
         |  count(distinct searchid) as cv
         |FROM
         |  base_data
         |GROUP BY unitid, userid, conversion_goal, click_hour_diff, cv_hour_diff
         |""".stripMargin
    println(sqlRequest4)
    val data = spark.sql(sqlRequest4).cache()

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