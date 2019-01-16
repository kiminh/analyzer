package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.ocpc.utils.OcpcUtils.getIdeaUpdates
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory


object OcpcCPAsuggestTable {
  def main(args: Array[String]): Unit = {
    /*
    每天根据是否有ocpc的广告记录更新cpc阶段的推荐cpa并存储到pb文件中
    1. 根据日期抽取前一天的推荐cpa
    2. 从ocpc_union_log_hourly判断是否有ocpc广告的点击记录，并将标签（ocpc_flag）关联到推荐cpa表上
    3. 将推荐cpa表与dl_cpc.ocpc_cpc_cpa_suggest_hourly进行外关联
    4. 根据ocpc_flag判断是否更新cpa_suggest和t
      a. 如果ocpc_flag=1即当天有ocpc广告记录，则更新cpa_suggest和t: t = 1/sqrt(day)
      b. 如果ocpc_flag=0或null，则不更新cpa_suggest和t

    结果表：dl_cpc.ocpc_cpa_suggest_hourly
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // 抽取数据，并关联cpasuggest与ocpcflag
    val cpaSuggest = getSuggestTable(date, hour, spark)
    val ocpcData = getOcpcFlag(date, hour, spark)
    val rawData = cpaSuggest
      .join(ocpcData, Seq("ideaid"), "left_outer")
      .select("ideaid", "unitid", "conversion_goal", "cpa_suggest", "ocpc_flag")
      .na.fill(0, Seq("ocpc_flag"))
      .withColumn("new_cpa", col("cpa_suggest"))
      .select("ideaid", "unitid", "conversion_goal", "new_cpa", "ocpc_flag")

    // 将cpasuggest与结果表外关联
    val prevTable = getPrevTable(date, hour, spark)

    // 根据ocpcflag选择是否更新cpasuggest与t
    val data = prevTable
      .join(rawData, Seq("ideaid", "unitid", "conversion_goal"), "outer")
      .select("ideaid", "unitid", "conversion_goal", "cpa_suggest", "t", "days", "new_cpa", "ocpc_flag")
      .na.fill(0, Seq("t", "days"))

    val resultDF = updateCPAsuggest(data, date, hour, spark)

    // 重新存取结果表
    resultDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_cpa_suggest_hourly")

    resultDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
      .repartition(10).write.mode("overwrite").saveAsTable("dl_cpc.ocpc_cpa_suggest_once")
  }

  def updateCPAsuggest(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    data.createOrReplaceTempView("base_table")

    val sqlRequest1 =
      s"""
         |SELECT
         |  ideaid,
         |  unitid,
         |  conversion_goal,
         |  (case when ocpc_flag=1 then cpa_suggest else new_cpa end) as new_cpa_suggest,
         |  (case when ocpc_flag=1 then days + 1 else 0 end) as days,
         |  new_cpa,
         |  cpa_suggest,
         |  ocpc_flag,
         |  t
         |FROM
         |  base_table
       """.stripMargin
    println(sqlRequest1)
    val rawData = spark
      .sql(sqlRequest1)
      .withColumn("new_t", sqrt(col("days")))

    rawData.createOrReplaceTempView("suggest_table")

    val sqlRequest2 =
      s"""
         |SELECT
         |  ideaid,
         |  unitid,
         |  conversion_goal,
         |  new_cpa_suggest as cpa_suggest,
         |  1.0 / new_t as t,
         |  days
         |FROM
         |  suggest_table
       """.stripMargin
    println(sqlRequest2)
    val resultDF = spark.sql(sqlRequest2)

    resultDF

  }

  def getSuggestTable(date: String, hour: String, spark: SparkSession) = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -1)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = s"`date`='$date1' and `hour` = '23' and version='qtt_demo'"

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  original_conversion,
         |  cpa
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("conversion_goal", col("original_conversion"))
      .groupBy("unitid", "conversion_goal")
      .agg(avg(col("cpa")).alias("cpa_suggest"))
      .select("unitid", "conversion_goal", "cpa_suggest")

    // 关联到ideaid
    val relationData = spark
      .table("dl_cpc.ocpc_ctr_data_hourly")
      .where(s"`date`='$date1'")
      .select("ideaid", "unitid")
      .distinct()

    // 关联
    val resultDF = data
      .join(relationData, Seq("unitid"), "left_outer")
      .select("ideaid", "unitid", "conversion_goal", "cpa_suggest")
      .filter("ideaid is not null")

    resultDF

  }

  def getOcpcFlag(date: String, hour: String, spark: SparkSession) = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -1)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = s"`dt`='$date1'"

    // 抽取ocpc_unionlog的数据
    val data = spark
      .table("dl_cpc.ocpc_unionlog")
      .where(selectCondition)
      .filter("isclick>1 and length(ocpc_log)>0")
      .withColumn("ocpc_flag", lit(1))
      .select("ideaid", "ocpc_flag")
      .distinct()

    data
  }

  def getPrevTable(date: String, hour: String, spark: SparkSession) = {
    var dayCnt = 1
    var prevTable = getPrevByHour(date, hour, dayCnt, spark)
    while (dayCnt < 11) {
      val cnt = prevTable.count()
      println(s"check prevTable Count: $cnt, at dayCnt = $dayCnt")
      if (cnt > 0) {
        dayCnt = 11
      } else {
        dayCnt += 1
        prevTable = getPrevByHour(date, hour, dayCnt, spark)
      }
    }
    prevTable
  }

  def getPrevByHour(date: String, hour: String, dayCnt: Int, spark: SparkSession) = {
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -dayCnt)
    val dt = calendar.getTime
    val date1 = sdf.format(dt)
    val selectCondition = s"`date`='$date1' and `hour` = '$hour' and version = 'qtt_demo'"

    // 抽取数据
    val data = spark
      .table("dl_cpc.ocpc_cpa_suggest_hourly")
      .where(selectCondition)

    data
  }

}

