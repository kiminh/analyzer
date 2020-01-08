package com.cpc.spark.oCPX.deepOcpc.permission

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.cpc.spark.oCPX.OcpcTools.mapMediaName
import com.typesafe.config.ConfigFactory
//import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.cpc.spark.ocpcV3.utils
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object OcpcDeepCalculateCV {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = args(2).toInt
    val deepConversionGoal = args(3).toInt
    val spark = SparkSession
      .builder()
      .appName(s"ocpc identifier auc: $date, $hour")
      .enableHiveSupport().getOrCreate()

    // 抽取数据
    val resultDF = OcpcDeepCalculateCVmain(date, hour, hourInt, deepConversionGoal, spark)

    resultDF
      .repartition(10)
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_unitid_auc_hourly_v2")
      .write.mode("overwrite").saveAsTable("test.ocpc_unitid_auc_hourly20191107b")
  }

  def OcpcDeepCalculateCVmain(date: String, hour: String, hourInt: Int, deepConversionGoal: Int, spark: SparkSession) = {
    // 抽取数据
    val data = getData(hourInt, date, hour, deepConversionGoal, spark)
    // 计算auc
    val resultDF = data
        .na.fill(0, Seq("iscvr"))
        .groupBy("identifier", "media", "deep_conversion_goal")
        .agg(
          sum(col("isclick")).alias("click"),
          sum(col("iscvr")).alias("cv"),
          sum(col("price")).alias("cost"),
          avg(col("deep_cpa")).alias("deep_cpagiven")
        )
        .withColumn("deep_cpareal", col("cost") / col("cv"))
        .select("identifier", "media", "deep_conversion_goal", "click", "cv", "cost", "deep_cpagiven", "deep_cpareal")


    resultDF
  }

  def getData(hourInt: Int, date: String, hour: String, deepConversionGoal: Int, spark: SparkSession) = {
    // 取历史数据
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
    val selectCondition1 = getTimeRangeSqlDate(date1, hour1, date, hour)
    // 取数据: score数据
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    cast(unitid as string) identifier,
         |    cast(deep_cvr as bigint) as score,
         |    media_appsid,
         |    (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |    end) as industry,
         |    deep_conversion_goal,
         |    price * 0.01 as price,
         |    deep_cpa * 0.01 as deep_cpa,
         |    isclick
         |from dl_cpc.ocpc_base_unionlog
         |where $selectCondition1
         |and isclick = 1
         |and is_ocpc = 1
         |and is_deep_ocpc = 1
         |and deep_cvr is not null
       """.stripMargin
    println(sqlRequest)
    val scoreData = spark
      .sql(sqlRequest)
      .withColumn("media", lit("all"))
      .filter(s"deep_conversion_goal = $deepConversionGoal")

//    val scoreData = mapMediaName(scoreDataRaw, spark)

    // 取历史区间: cvr数据
    val selectCondition2 = s"`date`>='$date1'"
    // 抽取数据
    val sqlRequest2 =
    s"""
       |SELECT
       |  searchid,
       |  label as iscvr,
       |  deep_conversion_goal
       |FROM
       |  dl_cpc.ocpc_label_deep_cvr_hourly
       |WHERE
       |  $selectCondition2
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2).distinct()


    // 关联数据
    val resultDF = scoreData
      .join(cvrData, Seq("searchid", "deep_conversion_goal"), "left_outer")
      .select("searchid", "identifier", "media", "deep_conversion_goal", "score", "iscvr", "industry", "price", "deep_cpa", "isclick")
      .na.fill(0, Seq("label"))
      .select("searchid", "identifier", "media", "deep_conversion_goal", "score", "iscvr", "industry", "price", "deep_cpa", "isclick")

    resultDF
  }
}