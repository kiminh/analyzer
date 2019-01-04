package com.cpc.spark.ocpcV3.ocpc.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils.getTimeRangeSql
import com.cpc.spark.ocpc.OcpcUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.functions._
import sun.java2d.loops.DrawGlyphListAA


object OcpcPIDwithCPAv2 {
  /*
  用于ocpc明投的相关代码
   */
  def main(args: Array[String]): Unit = {
    /*
    根据PID控制调整k值：
    1. 获得历史k值
    2. 获得历史cpa
    3. 根据给定cpa计算cpa_ratio
    4. 更新k值
     */
    val spark = SparkSession.builder().appName("OcpcPIDwithCPA").enableHiveSupport().getOrCreate()

    // bash: 2019-01-02 12 version2 novel
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    var mediaSelection = s"media_appsid in ('80000001', '80000002')"
    if (media == "qtt") {
      mediaSelection = s"media_appsid in ('80000001', '80000002')"
    } else {
      mediaSelection = s"media_appsid in ('80001098','80001292')"
    }

    // TODO 表名
    val prevTable = spark
      .table("test.ocpc_prev_pb_hourly")
      .where(s"version='$version'")
    val cvrData = getCVR1data(date, hour, spark)

    val historyData = getHistory(mediaSelection, date, hour, spark)
    val kvalue = getHistoryK(historyData, prevTable, date, hour, spark)
    val cpaHistory = getCPAhistory(historyData, cvrData, 1, date, hour, spark)
    val cpaRatio = calculateCPAratio(cpaHistory, date, hour, spark)
    val result = updateK(kvalue, cpaRatio, date, hour, spark)
    val resultDF = result
      .select("identifier", "k_value", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_pid_k_hourly")
//    resultDF
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_pid_k_hourly")


  }

  def getHistory(mediaSelection: String, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -24)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  cast(unitid as string) identifier,
         |  ext['adclass'].int_value as adclass,
         |  isshow,
         |  isclick,
         |  price,
         |  ocpc_log,
         |  ocpc_log_dict,
         |  ocpc_log_dict['kvalue'] as kvalue,
         |  ocpc_log_dict['cpagiven'] as cpagiven,
         |  hour
         |FROM
         |  dl_cpc.ocpc_union_log_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  $mediaSelection
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF
  }

  def getCVR1data(date: String, hour: String, spark: SparkSession) = {
    /*
    根据需要调整获取cvr的函数
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -24)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition = getTimeRangeSql2(date1, hour1, date, hour)

    val resultDF = spark
      .table("dl_cpc.ml_cvr_feature_v1")
      .where(selectCondition)
      .filter(s"label_type!=12")
      .withColumn("iscvr", col("label2"))
      .select("searchid", "iscvr")
      .filter("iscvr=1")
      .distinct()
    resultDF
  }

  def getHistoryK(historyData: DataFrame, prevPb: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /**
      * 计算修正前的k基准值
      * case1：前6个小时有isclick=1的数据，统计这批数据的k均值作为基准值
      * case2：前6个小时没有isclick=1的数据，将前一个小时的数据作为基准值
      */
    // case1
    val case1 = historyData
      .filter("isclick=1")
      .groupBy("identifier")
      .agg(avg(col("kvalue")).alias("kvalue1"))
      .select("identifier", "kvalue1")

    // case2
    val case2 = prevPb
      .withColumn("kvalue2", col("kvalue"))
      .select("identifier", "kvalue2")
      .distinct()

    // 优先case1，然后case2，最后case3
    val resultDF = case1
      .join(case2, Seq("identifier"), "outer")
      .select("identifier", "kvalue1", "kvalue2")
      .withColumn("kvalue", when(col("kvalue1").isNull, col("kvalue2")).otherwise(col("kvalue1")))

    resultDF
  }

  def getCPAhistory(historyData: DataFrame, cvrRaw: DataFrame, conversionGoal: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    计算cpa_history，分为cvr2和cvr3
    1. 获取cost
    2. 获取cvr
    3. 计算cpa_history
     */

    // cost data
    val costData = historyData
      .filter("isclick=1")
      .groupBy("identifier")
      .agg(
        sum(col("price")).alias("cost"),
        avg(col("cpagiven")).alias("cpagiven"))
      .select("identifier", "cost", "cpagiven")

    // cvr data
    // 用searchid关联
    val cvrData = historyData
      .join(cvrRaw, Seq("searchid"), "left_outer")
      .groupBy("identifier")
      .agg(sum(col("iscvr")).alias("cvrcnt"))
      .select("identifier", "cvrcnt")

    // 计算cpa
    val resultDF = costData
      .join(cvrData, Seq("identifier"), "left_outer")
      .withColumn("cpa", col("cost") * 1.0 / col("cvrcnt"))
      .withColumn("conversion_goal", lit(conversionGoal))
      .select("identifier", "cpa", "cvrcnt", "cost", "cpagiven", "conversion_goal")

    resultDF
  }

  def calculateCPAratio(cpaHistory: DataFrame, date: String, hour: String, spark: SparkSession) = {
    cpaHistory.createOrReplaceTempView("raw_table")

    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  cpagiven,
         |  cost,
         |  cvrcnt,
         |  cpa,
         |  (case when cpagiven is null then 1.0
         |        when cvrcnt is null or cvrcnt = 0 then 0.8
         |        when cvrcnt>0 then cpagiven * 1.0 / cpa
         |        else 1.0 end) as cpa_ratio,
         |   conversion_goal
         |FROM
         |  raw_table
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF

  }

  def updateK(kvalue: DataFrame, cpaRatio: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /**
      * 根据新的K基准值和cpa_ratio来在分段函数中重新定义k值
      * case1：0.9 <= cpa_ratio <= 1.1，k基准值
      * case2：0.8 <= cpa_ratio < 0.9，k / 1.1
      * case2：1.1 < cpa_ratio <= 1.2，k * 1.1
      * case3：0.6 <= cpa_ratio < 0.8，k / 1.2
      * case3：1.2 < cpa_ratio <= 1.4，k * 1.2
      * case4：0.4 <= cpa_ratio < 0.6，k / 1.4
      * case5：1.4 < cpa_ratio <= 1.6，k * 1.4
      * case6：cpa_ratio < 0.4，k / 1.6
      * case7：cpa_ratio > 1.6，k * 1.6
      *
      * 上下限依然是0.2 到1.2
      */

    // 关联得到基础表
    val rawData = kvalue
      .join(cpaRatio, Seq("identifier"), "outer")
      .select("identifier", "cpa_ratio", "conversion_goal", "kvalue")

    val resultDF = rawData
      .withColumn("ratio_tag", udfSetRatioCase()(col("cpa_ratio")))
      .withColumn("updated_k", udfUpdateK()(col("ratio_tag"), col("kvalue")))
      .withColumn("k_value", col("updated_k"))


    resultDF

  }

}


