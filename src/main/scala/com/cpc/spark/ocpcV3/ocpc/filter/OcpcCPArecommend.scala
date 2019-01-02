package com.cpc.spark.ocpcV3.ocpc.filter

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object OcpcCPArecommend{
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession
      .builder()
      .appName(s"ocpc cpc stage data: $date, $hour")
      .enableHiveSupport().getOrCreate()

    // 计算costData和cvrData
    val costData = getCost(date, hour, spark)
    val cvr1Data = getCVR("cvr1", date, hour, spark)
    val cvr2Data = getCVR("cvr2", date, hour, spark)
    val cvr3Data = getCVR("cvr3", date, hour, spark)

    val cpa1 = calculateCPA(costData, cvr1Data, date, hour, spark)
    val cpa2 = calculateCPA(costData, cvr2Data, date, hour, spark)
    val cpa3 = calculateCPA(costData, cvr3Data, date, hour, spark)

    // 调整字段
    val cpa1Data = cpa1.withColumn("conversion_goal", lit(1))
    val cpa2Data = cpa2.withColumn("conversion_goal", lit(2))
    val cpa3Data = cpa3.withColumn("conversion_goal", lit(3))

    val cpaData = cpa1Data
      .union(cpa2Data)
      .union(cpa3Data)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    cpaData.write.mode("overwrite").saveAsTable("test.ocpc_qtt_cpa_recommend_hourly")

  }

  def getCost(date: String, hour: String, spark: SparkSession) = {
    // 取历史区间
    val hourCnt = 72
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // 取数据
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  unitid,
         |  userid,
         |  total_price
         |FROM
         |  dl_cpc.ocpc_ctr_data_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println("############## getCost function ###############")
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .groupBy("ideaid", "unitid", "userid")
      .agg(sum(col("total_price")).alias("cost"))
      .select("ideaid", "unitid", "userid", "cost")

    resultDF
  }

  def getCVR(cvrType: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史区间
    val hourCnt = 72
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    // 取数据
    val tableName = "dl_cpc.ocpcv3_" + cvrType + "_data_hourly"
    println(s"table name is: $tableName")
    val resultDF = spark
      .table(tableName)
      .where(selectCondition)
      .groupBy("ideaid")
      .agg(sum(col(cvrType + "_cnt")).alias("cvrcnt"))
      .select("ideaid", "cvrcnt")
      .filter("cvrcnt>0")


    resultDF
  }

  def calculateCPA(costData: DataFrame, cvrData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val resultDF = costData
      .join(cvrData, Seq("ideaid"), "inner")
      .filter("cvrcnt is not null and cvrcnt>0")
      .withColumn("cpa", col("cost") * 1.0 / col("cvrcnt"))
      .select("ideaid", "unitid", "userid", "cpa")
      .filter("cpa is not null and cpa > 0")

    resultDF
  }

}