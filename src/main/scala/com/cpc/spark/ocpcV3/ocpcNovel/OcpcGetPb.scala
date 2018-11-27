package com.cpc.spark.ocpcV3.ocpcNovel

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object OcpcGetPb {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val cvrData = getCvr(date, hour, spark)
    val kvalue = getK(date, hour, spark)
    val cpaHistory = getCPAhistory(date, hour, spark)

//    val data = cvrData.join(kvalue, )

  }

  def getK(date: String, hour: String, spark: SparkSession) = {
    val tableName = "test.ocpc_v3_novel_k_regression"
    val rawData = spark
      .table(tableName)
      .where(s"`date`='$date' and `hour`='$hour'")
    rawData.show(10)

    val resultDF = rawData
      .select("unitid")
      .distinct()
      .withColumn("kvalue", lit(0.5))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }

  def getCPAhistory(date: String, hour: String, spark: SparkSession) = {
    val tableName = "test.ocpcv3_novel_cpa_history_hourly"
    val resultDF = spark
      .table(tableName)
      .where(s"`date`='$date' and `hour`='$hour'")
    resultDF.show(10)

    resultDF
  }

  def getCvr(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -72)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // cvr data
    // cvr1 or cvr3 data
    val sqlRequestCvr1Data =
    s"""
       |SELECT
       |  unitid,
       |  cvr1_cnt
       |FROM
       |  dl_cpc.ocpcv3_cvr1_data_hourly
       |WHERE
       |  $selectCondition
       |AND
       |  media_appsid in ("80001098","80001292")
       """.stripMargin
    println(sqlRequestCvr1Data)
    val cvr1Data = spark
      .sql(sqlRequestCvr1Data)
      .groupBy("unitid")
      .agg(sum(col("cvr1_cnt")).alias("cvr1cnt"))
    cvr1Data.show(10)

    // cvr2data
    val sqlRequestCvr2Data =
      s"""
         |SELECT
         |  unitid,
         |  cvr2_cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr2_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80001098","80001292")
       """.stripMargin
    println(sqlRequestCvr2Data)
    val cvr2Data = spark
      .sql(sqlRequestCvr2Data)
      .groupBy("unitid")
      .agg(sum(col("cvr2_cnt")).alias("cvr2cnt"))
    cvr2Data.show(10)

    // 数据关联
    val resultDF = cvr1Data
      .join(cvr2Data, Seq("unitid"), "outer")
      .withColumn("cvr1cnt", when(col("cvr1cnt").isNull, 0).otherwise(col("cvr1cnt")))
      .withColumn("cvr2cnt", when(col("cvr2cnt").isNull, 0).otherwise(col("cvr2cnt")))
      .select("unitid", "cvr1cnt", "cvr2cnt")

    // 返回结果
    resultDF.show(10)
    resultDF
  }


}
