package com.cpc.spark.oCPX.oCPC.assessment

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.oCPX.OcpcTools.{getTimeRangeSqlDate, mapMediaName}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcCalibration_assessment {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt
    val dbName = args(3).toString
    println("parameters:")
    println(s"date=$date")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcShallowCVrecall_predict: $date").enableHiveSupport().getOrCreate()

    val data = pcocCalibrationAssessment(date, hour, hourInt, spark)

    data
      .write.mode("overwrite").saveAsTable("test.ocpc_shallow_calibration_assessment20200207a")


  }

  def pcocCalibrationAssessment(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    val pcocData = getPcoc(date, hour, hourInt, "qtt", spark)
    val caliData = getCalibrationValue(date, hour, 4, hourInt, "Qtt", spark)

    pcocData
      .write.mode("overwrite").saveAsTable("test.ocpc_shallow_calibration_assessment20200207b")

    val data = pcocData
      .join(caliData, Seq("unitid", "conversion_goal", "hour_diff"), "inner")
      .select("unitid", "userid", "conversion_goal", "hour_diff", "date", "hour", "cali_date", "cali_hour", "pre_cvr", "post_cvr", "pcoc", "cost", "cv", "cali_value", "exp_tag")
      .withColumn("cali_pcoc", col("pcoc") * col("cali_value"))

    data
  }

  def getCalibrationValue(date: String, hour: String, hourDiff: Int, hourInt: Int, media: String, spark: SparkSession) = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    calendar.add(Calendar.HOUR, -hourDiff)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  cast(identifier as int) unitid,
         |  conversion_goal,
         |  exp_tag,
         |  cali_value,
         |  date,
         |  hour
         |FROM
         |  dl_cpc.ocpc_param_pb_data_hourly_alltype
         |WHERE
         |  $selectCondition
         |AND
         |  version = 'ocpcv1pbfile'
         |AND
         |  exp_tag like '%$media'
         |""".stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("hour_diff", udfCalculateHourDiff(date1, hour1)(col("date"), col("hour")))
      .withColumn("cali_date", col("date"))
      .withColumn("cali_hour", col("hour"))
      .select("unitid", "conversion_goal", "exp_tag", "cali_value", "hour_diff", "cali_date", "cali_hour")
      .distinct()

    data

  }

  def getPcoc(date: String, hour: String, hourInt: Int, media: String, spark: SparkSession) = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourInt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSqlDate(date1, hour1, date, hour)

    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    conversion_goal,
         |    conversion_from,
         |    media_appsid,
         |    price,
         |    exp_cvr,
         |    isclick,
         |    date,
         |    hour
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    $selectCondition
         |AND
         |    is_ocpc = 1
         |AND
         |    isclick = 1
         |""".stripMargin
    println(sqlRequest1)
    val clickDataRaw = spark.sql(sqlRequest1)
    val clickData = mapMediaName(clickDataRaw, spark)

    val sqlRequest2 =
      s"""
         |SELECT
         |    searchid,
         |    conversion_goal,
         |    conversion_from,
         |    1 as iscvr
         |FROM
         |    dl_cpc.ocpc_cvr_log_hourly
         |WHERE
         |    $selectCondition
         |""".stripMargin
    println(sqlRequest2)
    val cvData = spark
      .sql(sqlRequest2)
      .select("searchid", "conversion_goal", "conversion_from", "iscvr")
      .distinct()

    val baseData = clickData
      .filter(s"media = '$media'")
      .join(cvData, Seq("searchid", "conversion_goal", "conversion_from"), "left")
      .select("searchid", "unitid", "userid", "conversion_goal", "conversion_from", "media",  "price", "exp_cvr", "isclick", "iscvr", "date", "hour")

    baseData.createOrReplaceTempView("base_data")

    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  date,
         |  hour,
         |  sum(case when isclick=1 then price else 0 end) * 0.01 as cost,
         |  sum(iscvr) as cv,
         |  sum(case when isclick=1 then exp_cvr else 0 end) * 1.0 / sum(isclick) as pre_cvr,
         |  sum(iscvr) * 1.0 / sum(isclick) as post_cvr
         |FROM
         |  base_data
         |GROUP BY unitid, userid, conversion_goal, date, hour
         |""".stripMargin
    println(sqlRequest3)
    val data = spark
      .sql(sqlRequest3)
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("hour_diff", udfCalculateHourDiff(date1, hour1)(col("date"), col("hour")))
      .cache()

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