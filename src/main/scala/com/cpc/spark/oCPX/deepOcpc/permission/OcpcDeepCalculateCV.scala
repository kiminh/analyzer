package com.cpc.spark.oCPX.deepOcpc.permission

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
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
    val version = args(2).toString
    val hourInt = args(3).toInt
    val spark = SparkSession
      .builder()
      .appName(s"ocpc identifier auc: $date, $hour")
      .enableHiveSupport().getOrCreate()

    // 抽取数据
    val resultDF = OcpcDeepCalculateCVmain(date, hour, hourInt, spark)

    resultDF
      .repartition(10)
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_unitid_auc_hourly_v2")
      .write.mode("overwrite").saveAsTable("test.ocpc_unitid_auc_hourly20191107b")
  }

  def OcpcDeepCalculateCVmain(date: String, hour: String, hourInt: Int, spark: SparkSession) = {
    // 抽取数据
    val data = getData(hourInt, date, hour, spark)
    // 计算auc
    val resultDF = data
        .na.fill(0, Seq("iscvr"))
        .groupBy("identifier", "media", "deep_conversion_goal")
        .agg(
          sum(col("iscvr")).alias("cv"),
          sum(col("price")).alias("cost")
        )
        .select("identifier", "media", "deep_conversion_goal", "cv", "cost")


    resultDF
  }

  def getData(hourInt: Int, date: String, hour: String, spark: SparkSession) = {
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

    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)
    // 取数据: score数据
    val sqlRequest =
      s"""
         |select
         |    searchid,
         |    cast(unitid as string) identifier,
         |    cast(deep_cvr as bigint) as score,
         |    (case
         |        when media_appsid in ('80000001', '80000002') then 'qtt'
         |        when media_appsid in ('80002819', '80004944', '80004948') then 'hottopic'
         |        else 'novel'
         |    end) as media,
         |    (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |    end) as industry,
         |    deep_conversion_goal,
         |    price * 0.01 as price,
         |    isclick
         |from dl_cpc.ocpc_base_unionlog
         |where $selectCondition1
         |and isclick = 1
         |and $mediaSelection
         |and is_ocpc = 1
         |and is_deep_ocpc = 1
         |and deep_cvr is not null
       """.stripMargin
    println(sqlRequest)
    val scoreData = spark.sql(sqlRequest)

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
      .select("searchid", "identifier", "media", "deep_conversion_goal", "score", "iscvr", "industry", "price")
      .na.fill(0, Seq("label"))
      .select("searchid", "identifier", "media", "deep_conversion_goal", "score", "iscvr", "industry", "price")

    resultDF
  }
}