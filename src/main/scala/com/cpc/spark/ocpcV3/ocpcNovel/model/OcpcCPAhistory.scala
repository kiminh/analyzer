package com.cpc.spark.ocpcV3.ocpcNovel.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object OcpcCPAhistory {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // TODO 测试
    val result = calculateCPA(date, hour, spark)
    result.write.mode("overwrite").saveAsTable("test.ocpcv3_cpa_bid_ratio20181127")
    println(s"succesfully save data into table: test.ocpcv3_cpa_bid_ratio20181127")
  }

  def calculateCPA(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    // cost数据
    val sqlRequestCostData =
      s"""
         |SELECT
         |  unitid,
         |  total_price,
         |  total_bid,
         |  ctr_cnt
         |FROM
         |  dl_cpc.ocpcv3_ctr_data_hourly
         |WHERE
         |  `date`='$date1'
         |AND
         |  media_appsid in ("80000001", "80000002")
       """.stripMargin
    println(sqlRequestCostData)
    val costData = spark
      .sql(sqlRequestCostData)
      .groupBy("unitid")
      .agg(
        sum(col("total_price")).alias("cost"),
        sum(col("total_bid")).alias("bid"),
        sum(col("ctr_cnt")).alias("ctrcnt"))
    costData.show(10)

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
         |  `date`='$date1'
         |AND
         |  media_appsid in ("80000001", "80000002")
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
         |  `date`='$date1'
         |AND
         |  media_appsid in ("80000001", "80000002")
       """.stripMargin
    println(sqlRequestCvr2Data)
    val cvr2Data = spark
      .sql(sqlRequestCvr2Data)
      .groupBy("unitid")
      .agg(sum(col("cvr2_cnt")).alias("cvr2cnt"))
    cvr2Data.show(10)

    // 关联数据
    val resultDF = costData
      .join(cvr1Data, Seq("unitid"), "left_outer")
      .join(cvr2Data, Seq("unitid"), "left_outer")
      .select("unitid", "cost", "cvr1cnt", "cvr2cnt", "bid", "ctrcnt")
      .withColumn("cpa1", col("cost") * 1.0 / col("cvr1cnt"))
      .withColumn("cpa2", col("cost") * 1.0 / col("cvr2cnt"))
      .withColumn("avg_bid", col("bid") * 1.0 / col("ctrcnt"))
      .withColumn("alpha1", col("cpa1") * 1.0 / col("avg_bid"))
      .withColumn("alpha2", col("cpa2") * 1.0 / col("avg_bid"))

    resultDF.show(10)
    resultDF

  }

//  def CPAbidRatio(date: String, hour: String, spark: SparkSession) = {
//    // 取历史数据
//    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
//    val today = dateConverter.parse(date)
//    val calendar = Calendar.getInstance
//    calendar.setTime(today)
//    calendar.add(Calendar.DATE, -1)
//    val yesterday = calendar.getTime
//    val date1 = dateConverter.format(yesterday)
//
//    // cost数据
//    val sqlRequestCostData =
//      s"""
//         |SELECT
//         |  unitid,
//         |  total_bid,
//         |  ctr_cnt
//         |FROM
//         |  dl_cpc.ocpcv3_ctr_data_hourly
//         |WHERE
//         |  `date`='$date1'
//         |AND
//         |  media_appsid in ("80000001", "80000002")
//       """.stripMargin
//    println(sqlRequestCostData)
//    val costData = spark
//      .sql(sqlRequestCostData)
//      .groupBy("unitid")
//      .agg(
//        sum(col("total_bid")).alias("bid"),
//        sum(col("ctr_cnt")))
//    costData.show(10)
//  }
}
