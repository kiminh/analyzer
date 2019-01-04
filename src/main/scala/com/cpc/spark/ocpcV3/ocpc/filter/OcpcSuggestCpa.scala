package com.cpc.spark.ocpcV3.ocpc.filter

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils.getTimeRangeSqlCondition
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}


object OcpcSuggestCpa{
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
      .select("ideaid", "userid", "adclass", "cpa", "cost", "click", "cvrcnt", "acb", "pcvr", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    cpaData.write.mode("overwrite").saveAsTable("test.ocpc_suggest_cpa_recommend_hourly")
    println("successfully save data into table: test.ocpc_suggest_cpa_recommend_hourly")

  }

  def getCost(date: String, hour: String, spark: SparkSession) = {
    // 取历史区间
    val hourCnt = 72
    val selectCondition = getTimeRangeSqlCondition(date, hour, hourCnt)

    // 取数据
    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  userid,
         |  adclass,
         |  total_price,
         |  ctr_cnt,
         |  total_bid,
         |  total_pcvr
         |FROM
         |  dl_cpc.ocpc_ctr_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80000001", "80000002")
       """.stripMargin
    println("############## getCost function ###############")
    println(sqlRequest)
    val resultDF = spark
      .sql(sqlRequest)
      .groupBy("ideaid", "userid", "adclass")
      .agg(sum(col("total_price")).alias("cost"),
        sum(col("ctr_cnt")).alias("click"),
        sum(col("total_bid")).alias("click_bid_sum"),
        sum(col("total_pcvr")).alias("click_pcvr_sum"))
      .select("ideaid", "userid", "adclass", "cost", "click", "click_bid_sum", "click_pcvr_sum")

    resultDF
  }

  def getCVR(cvrType: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史区间
    val hourCnt = 72
	val selectCondition = getTimeRangeSqlCondition(date, hour, hourCnt)

    // 取数据
    val tableName = "dl_cpc.ocpcv3_" + cvrType + "_data_hourly"
    println(s"table name is: $tableName")
    val resultDF = spark
      .table(tableName)
      .where(selectCondition)
      .filter(s"media_appsid in ('80000001', '80000002')")
      .groupBy("ideaid", "adclass")
      .agg(sum(col(cvrType + "_cnt")).alias("cvrcnt"))
      .select("ideaid", "adclass", "cvrcnt")
      .filter("cvrcnt>30 and cvrcnt is not null")


    resultDF
  }

  def calculateCPA(costData: DataFrame, cvrData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val resultDF = costData
      .join(cvrData, Seq("ideaid", "adclass"), "inner")
      .filter("cvrcnt is not null and cvrcnt>0")
      .withColumn("cpa", col("cost") * 1.0 / col("cvrcnt"))
      .withColumn("acb", col("click_bid_sum") * 1.0 / col("click"))
      .withColumn("pcvr", col("click_pcvr_sum") * 1.0 / col("click"))
      .select("ideaid", "userid", "adclass", "cpa", "cost", "cvrcnt", "click", "acb", "pcvr")
      .filter("cpa is not null and cpa > 0")

    resultDF
  }

}
