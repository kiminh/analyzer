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


object OcpcPIDwithCPA {
  def main(args: Array[String]): Unit = {
    /*
    根据PID控制调整k值：
    1. 获得历史k值
    2. 获得历史cpa
    3. 根据给定cpa计算cpa_ratio
    4. 更新k值
     */
    val spark = SparkSession.builder().appName("OcpcPIDwithCPA").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    // TODO 表名
    val prevTable = spark
      .table("test.ocpc_prev_pb")


    val historyData = getHistory(date, hour, spark)
    val kvalue = getHistoryK(historyData, prevTable, date, hour, spark)
    val cpaHistory = getCPAhistory(historyData, date, hour, spark)
    val cpaRatio = calculateCPAratio(cpaHistory, date, hour, spark)
    val result = updateK(kvalue, cpaRatio, date, hour, spark)
    val resultDF = result
      .select("identifier", "k_value", "conversion_goal")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("v1"))

//    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_pid_k_hourly")
    resultDF
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_pid_k_hourly")


  }

  def getHistory(date: String, hour: String, spark: SparkSession) = {
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
         |  media_appsid in ('80000001', '80000002')
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
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

  def getCPAhistory(historyData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    计算cpa_history，分为cvr2和cvr3
    1. 获取cost
    2. 获取cvr2
    3. 获取cvr3
    4. 分别计算cpa_history
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

    val rawCvr1 = spark
      .table("dl_cpc.ml_cvr_feature_v1")
      .where(selectCondition)
      .withColumn("iscvr1", col("label2"))
      .select("searchid", "iscvr1")
      .filter("iscvr1=1")
      .distinct()

    val rawCvr2 = spark
      .table("dl_cpc.ml_cvr_feature_v2")
      .where(selectCondition)
      .withColumn("iscvr2", col("label"))
      .select("searchid", "iscvr2")
      .filter("iscvr2=1")
      .distinct()

    // cvr1
    val cvr1Data = historyData
      .join(rawCvr1, Seq("searchid"), "left_outer")
      .groupBy("identifier")
      .agg(sum(col("iscvr1")).alias("cvrcnt"))
      .select("identifier", "cvrcnt")

    // cvr2
    val cvr2Data = historyData
      .join(rawCvr2, Seq("searchid"), "left_outer")
      .groupBy("identifier")
      .agg(sum(col("iscvr2")).alias("cvrcnt"))
      .select("identifier", "cvrcnt")

    // 计算cpa
    // cvr1
    val cpa1 = costData
      .join(cvr1Data, Seq("identifier"), "left_outer")
      .withColumn("cpa", col("cost") * 1.0 / col("cvrcnt"))
      .withColumn("conversion_goal", lit(1))
      .select("identifier", "cpa", "cvrcnt", "cost", "cpagiven", "conversion_goal")
    // cvr2
    val cpa2 = costData
      .join(cvr2Data, Seq("identifier"), "left_outer")
      .withColumn("cpa", col("cost") * 1.0 / col("cvrcnt"))
      .withColumn("conversion_goal", lit(2))
      .select("identifier", "cpa", "cvrcnt", "cost", "cpagiven", "conversion_goal")

    val resultDF = cpa1.union(cpa2)

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

