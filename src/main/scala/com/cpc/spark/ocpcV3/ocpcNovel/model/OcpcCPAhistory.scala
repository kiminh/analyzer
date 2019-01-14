package com.cpc.spark.ocpcV3.ocpcNovel.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.ocpc.utils.OcpcUtils.getIdeaUpdates
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


object OcpcCPAhistory {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val cpaList = calculateCPA(date, hour, spark)
//    cpaList.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_cpaList")
    val result = checkCPA(cpaList, date, hour, spark)
//    dl_cpc.ocpcv3_novel_cpa_history_hourly
    val tableName = "dl_cpc.ocpcv3_novel_cpa_history_hourly"
//    result.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_cpa_history_hourly")
    result
      .repartition(10).write.mode("overwrite").insertInto(tableName)
    println(s"succesfully save data into table: $tableName")
  }

  def calculateCPA(date: String, hour: String, spark: SparkSession) = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -1)
    val start_date = calendar.getTime
    val date1 = sdf.format(start_date)
//    val selectCondition = getTimeRangeSql2(date1, hour, date, hour)
    val selectCondition = s"`date`='$date1'"

    // cost数据
    val sqlRequestCostData =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  total_price,
         |  total_bid,
         |  ctr_cnt
         |FROM
         |  dl_cpc.ocpcv3_ctr_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80000001", "80000002")
       """.stripMargin
    println(sqlRequestCostData)
    val costData = spark
      .sql(sqlRequestCostData)
      .groupBy("unitid", "adclass")
      .agg(
        sum(col("total_price")).alias("total_cost"),
        sum(col("total_bid")).alias("total_bid"),
        sum(col("ctr_cnt")).alias("ctrcnt"))
    costData.show(10)
//    costData.write.mode("overwrite").saveAsTable("test.ocpcv3_cpa_history_costdata")

    // cvr data
    // cvr1 or cvr3 data
    val sqlRequestCvr1Data =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  cvr1_cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr1_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80000001", "80000002")
       """.stripMargin
    println(sqlRequestCvr1Data)
    val cvr1Data = spark
      .sql(sqlRequestCvr1Data)
      .groupBy("unitid", "adclass")
      .agg(sum(col("cvr1_cnt")).alias("cvr1cnt"))
    cvr1Data.show(10)
//    cvr1Data.write.mode("overwrite").saveAsTable("test.ocpcv3_cpa_history_cvr1data")

    // cvr2data
    val sqlRequestCvr2Data =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  cvr2_cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr2_data_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  media_appsid in ("80000001", "80000002")
       """.stripMargin
    println(sqlRequestCvr2Data)
    val cvr2Data = spark
      .sql(sqlRequestCvr2Data)
      .groupBy("unitid", "adclass")
      .agg(sum(col("cvr2_cnt")).alias("cvr2cnt"))
    cvr2Data.show(10)
//    cvr2Data.write.mode("overwrite").saveAsTable("test.ocpcv3_cpa_history_cvr2data")

    // 关联数据
    val resultDF = costData
      .join(cvr1Data, Seq("unitid", "adclass"), "left_outer")
      .join(cvr2Data, Seq("unitid", "adclass"), "left_outer")
      .select("unitid", "adclass", "total_cost", "cvr1cnt", "cvr2cnt", "total_bid", "ctrcnt")
      .withColumn("cpa1", col("total_cost") * 1.0 / col("cvr1cnt"))
      .withColumn("cpa2", col("total_cost") * 1.0 / col("cvr2cnt"))
      .withColumn("avg_bid", col("total_bid") * 1.0 / col("ctrcnt"))
      .withColumn("alpha1", col("cpa1") * 1.0 / col("avg_bid"))
      .withColumn("alpha2", col("cpa2") * 1.0 / col("avg_bid"))

    resultDF.show(10)
    resultDF

  }

  def checkCPA(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // 分别按cvr1和cvr2抽取数据
    // 过滤掉只有一个cvr的数据记录
    // 取alpha的80%分位数

    // 取数据并过滤
    val cvr1Data = data
      .select("unitid", "adclass", "cvr1cnt", "alpha1", "avg_bid", "cpa1")
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .filter("cvr1cnt>1")
    cvr1Data.createOrReplaceTempView("cvr1_data")
    val cvr2Data = data
      .select("unitid", "adclass", "cvr2cnt", "alpha2", "avg_bid", "cpa2")
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .filter("cvr2cnt>1")
    cvr2Data.createOrReplaceTempView("cvr2_data")

    // adclass cpa
    // TODO 考虑使用cost / cvr 来计算平均cpa
    val cvr1AdclassData = cvr1Data
      .groupBy("new_adclass")
      .agg(avg(col("cpa1")).alias("avg_cpa1"))
//    cvr1AdclassData.write.mode("overwrite").saveAsTable("test.ocpcv3_cvr1_adclass_cpa")
    val cvr2AdclassData = cvr2Data
      .groupBy("new_adclass")
      .agg(avg(col("cpa2")).alias("avg_cpa2"))
//    cvr2AdclassData.write.mode("overwrite").saveAsTable("test.ocpcv3_cvr2_adclass_cpa")
    val adclassCPA = cvr1AdclassData
      .join(cvr2AdclassData, Seq("new_adclass"), "outer")
      .select("new_adclass", "avg_cpa1", "avg_cpa2")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
//    adclassCPA.write.mode("overwrite").saveAsTable("test.ocpcv3_adclass_cpa_history_hourly")
    adclassCPA
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpcv3_adclass_cpa_history_hourly")

    // 取分位数
    // cvr1
    val sqlRequest1 =
      s"""
         |SELECT
         |  new_adclass,
         |  percentile(alpha1, 0.8) as alpha1_max
         |FROM
         |  cvr1_data
         |GROUP BY new_adclass
       """.stripMargin
    println(sqlRequest1)
    val cvr1Alpha = spark.sql(sqlRequest1)
    cvr1Alpha.show(10)
    val cvr1Result = cvr1Data
      .join(cvr1Alpha, Seq("new_adclass"))
      .select("unitid", "adclass", "cvr1cnt", "alpha1", "avg_bid", "cpa1", "new_adclass", "alpha1_max")
      .withColumn("cpa1_max", col("avg_bid") * col("alpha1_max"))
      .withColumn("cpa1_history", when(col("cpa1")>col("cpa1_max"), col("cpa1_max")).otherwise(col("cpa1")))
//    cvr1Result.write.mode("overwrite").saveAsTable("test.ocpc_cpa1_result_hourly")

    // cvr2
    val sqlRequest2 =
      s"""
         |SELECT
         |  new_adclass,
         |  percentile(alpha2, 0.8) as alpha2_max
         |FROM
         |  cvr2_data
         |GROUP BY new_adclass
       """.stripMargin
    println(sqlRequest2)
    val cvr2Alpha = spark.sql(sqlRequest2)
    cvr2Alpha.show(10)
    val cvr2Result = cvr2Data
      .join(cvr2Alpha, Seq("new_adclass"))
      .select("unitid", "adclass", "cvr2cnt", "alpha2", "avg_bid", "cpa2", "new_adclass", "alpha2_max")
      .withColumn("cpa2_max", col("avg_bid") * col("alpha2_max"))
      .withColumn("cpa2_history", when(col("cpa2")>col("cpa2_max"), col("cpa2_max")).otherwise(col("cpa2")))
//    cvr2Result.write.mode("overwrite").saveAsTable("test.ocpc_cpa2_result_hourly")

    // 关联结果
    val result = data
      .join(cvr1Result, Seq("unitid", "adclass"), "left_outer")
      .join(cvr2Result, Seq("unitid", "adclass"), "left_outer")
      .join(adclassCPA, Seq("new_adclass"), "left_outer")
      .withColumn("conversion_goal", when(col("cpa2_history").isNull, 1).otherwise(2))
      .withColumn("cpa1_history", when(col("alpha1_max").isNull, col("avg_cpa1")).otherwise(col("cpa1_history")))
      .withColumn("cpa2_history", when(col("alpha2_max").isNull, col("avg_cpa2")).otherwise(col("cpa2_history")))
      .select("unitid", "adclass", "cpa1_history", "cpa2_history", "conversion_goal")
      .withColumn("cpa1_history", when(col("cpa1_history").isNull, -1).otherwise(col("cpa1_history")))
      .withColumn("cpa2_history", when(col("cpa2_history").isNull, -1).otherwise(col("cpa2_history")))
      .withColumn("cpa_history", when(col("conversion_goal") === 1, col("cpa1_history")).otherwise(col("cpa2_history")))
      .withColumn("cpa_history", when(col("cpa_history") > 50000, 50000).otherwise(col("cpa_history")))
//    result.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_cpa_history_debug")

    val resultDF = result
      .select("unitid", "adclass", "cpa_history", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
    resultDF
  }

}
