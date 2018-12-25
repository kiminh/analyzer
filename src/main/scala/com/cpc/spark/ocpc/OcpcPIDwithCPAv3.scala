package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Utils.getTimeRangeSql
import com.cpc.spark.ocpc.OcpcUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.functions._
import sun.java2d.loops.DrawGlyphListAA


object OcpcPIDwithCPAv3 {
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

//    test.ocpc_k_value_table_ + hour
    // TODO prevTable 更换
    val prevTable = spark
      .table("dl_cpc.ocpc_pb_result_table_v6")
      .where(s"`date`='2018-12-24' and `hour`='11'")

    val advIdeaid = spark.table("test.ocpc_idea_update_time_" + hour)

    val historyData = getHistory(date, hour, spark)
    val kvalue = getHistoryK(historyData, prevTable, date, hour, spark)
    val cpaHistory = getCPAhistory(historyData, date, hour, spark)
    val cpaRatio = calculateCPAratio(cpaHistory, advIdeaid, date, hour, spark)
    val resultDF = updateK(kvalue, cpaRatio, date, hour, spark)

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_k_value_table_20181225")


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
    val selectCondition = getTimeRangeSql3(date1, hour1, date, hour)

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  adclass,
         |  isshow,
         |  isclick,
         |  iscvr,
         |  price,
         |  ocpc_log,
         |  ocpc_log_dict,
         |  ocpc_log_dict['kvalue'] as kvalue,
         |  hour
         |FROM
         |  dl_cpc.ocpc_unionlog
         |WHERE
         |  $selectCondition
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
      .groupBy("ideaid", "adclass")
      .agg(avg(col("kvalue")).alias("kvalue1"))
      .select("ideaid", "adclass", "kvalue1")

    // case2
    val case2 = prevPb
      .withColumn("kvalue2", col("k_value"))
      .select("ideaid", "adclass", "kvalue2")
      .distinct()

    // 优先case1，然后case2，最后case3
    val resultDF = case1
      .join(case2, Seq("ideaid", "adclass"), "outer")
      .select("ideaid", "adclass", "kvalue1", "kvalue2")
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
      .groupBy("ideaid", "adclass")
      .agg(sum(col("price")).alias("cost"))
      .select("ideaid", "adclass", "cost")

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

    val rawCvr2 = spark
      .table("dl_cpc.ml_cvr_feature_v1")
      .where(selectCondition)
      .withColumn("iscvr2", col("label2"))
      .select("searchid", "iscvr2")
      .filter("iscvr2=1")
      .distinct()

    val rawCvr3 = spark
      .table("dl_cpc.ml_cvr_feature_v2")
      .where(selectCondition)
      .withColumn("iscvr3", col("label"))
      .select("searchid", "iscvr3")
      .filter("iscvr3=1")
      .distinct()

    // cvr2
    val cvr2Data = historyData
      .join(rawCvr2, Seq("searchid"), "left_outer")
      .groupBy("ideaid", "adclass")
      .agg(sum(col("iscvr2")).alias("cvr2cnt"))
      .select("ideaid", "adclass", "cvr2cnt")

    // cvr3
    val cvr3Data = historyData
      .join(rawCvr3, Seq("searchid"), "left_outer")
      .groupBy("ideaid", "adclass")
      .agg(sum(col("iscvr3")).alias("cvr3cnt"))
      .select("ideaid", "adclass", "cvr3cnt")

    // 计算cpa
    // cvr2
    val cpa2 = costData
      .join(cvr2Data, Seq("ideaid", "adclass"), "left_outer")
      .withColumn("cpa2", col("cost") * 1.0 / col("cvr2cnt"))
      .select("ideaid", "adclass", "cpa2", "cvr2cnt")
    // cvr3
    val cpa3 = costData
      .join(cvr3Data, Seq("ideaid", "adclass"), "left_outer")
      .withColumn("cpa3", col("cost") * 1.0 / col("cvr3cnt"))
      .select("ideaid", "adclass", "cpa3", "cvr3cnt")

    val resultDF = cpa2
      .join(cpa3, Seq("ideaid", "adclass"), "outer")
      .select("ideaid", "adclass", "cpa2", "cpa3", "cvr2cnt", "cvr3cnt")

    resultDF
  }

  def calculateCPAratio(cpaHistory: DataFrame, advIdeaid: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val rawData = advIdeaid
      .join(cpaHistory, Seq("ideaid"), "left_outer")
      .select("ideaid", "adclass", "cpa2", "cpa3", "cvr2cnt", "cvr3cnt", "cpa_given")
    rawData.createOrReplaceTempView("raw_table")

    val sqlRequest =
      s"""
         |SELECT
         |  ideaid,
         |  adclass,
         |  cpa_given,
         |  cvr2cnt,
         |  cvr3cnt,
         |  cpa2,
         |  cpa3,
         |  (case when cpa_given is null then 1.0
         |        when cvr2cnt is null or cvr2cnt = 0 then 0.8
         |        when cvr2cnt>0 then cpa_given * 1.0 / cpa2
         |        else 1.0 end) as cpa2_ratio,
         |  (case when cpa_given is null then 1.0
         |        when cvr3cnt is null or cvr3cnt = 0 then 0.8
         |        when cvr3cnt>0 then cpa_given * 1.0 / cpa3
         |        else 1.0 end) as cpa3_ratio
         |FROM
         |  raw_table
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    // TODO 临时表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_cpa_ratio20181225")
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
    //    baseData.write.mode("overwrite").saveAsTable("test.ocpc_base_table")
    val rawData = kvalue
      .join(cpaRatio, Seq("ideaid", "adclass"), "outer")
      .select("ideaid", "adclass", "cpa2_ratio", "cpa3_ratio", "kvalue")

    val resultDF = rawData
      .withColumn("ratio_tag_cvr2", udfSetRatioCase()(col("cpa2_ratio")))
      .withColumn("ratio_tag_cvr3", udfSetRatioCase()(col("cpa3_ratio")))
      .withColumn("updated_k2", udfUpdateK()(col("cpa2_ratio"), col("kvalue")))
      .withColumn("updated_k3", udfUpdateK()(col("cpa3_ratio"), col("kvalue")))
      .withColumn("k_value2", col("updated_k2"))
      .withColumn("k_value3", col("updated_k3"))

    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_updated_k_table20181225")

    resultDF

  }

}

