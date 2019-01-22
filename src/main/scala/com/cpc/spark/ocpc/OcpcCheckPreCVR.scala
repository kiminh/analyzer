package com.cpc.spark.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcCheckPreCVR {
  def main(args: Array[String]): Unit = {
    /*
    抽取推荐cpa中的unitid，比较昨天和今天的平均预测cvr差距
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString

    // 抽取推荐cpa的unitid
    val data = getSuggestUnitid(date, hour, spark)
    // 根据slim_unionlog计算平均预测cvr
    val result = cmpPreCvr(data, date, hour, spark)

    result.repartition(1).write.mode("overwrite").saveAsTable("test.ocpc_check_pcvr20190122")
  }

  def cmpPreCvr(unitidList: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    // slim_unionlog数据
    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  unitid,
         |  exp_cvr,
         |  dt as date
         |FROM
         |  dl_cpc.slim_union_log
         |WHERE
         |  (`dt`='$date1' or `dt`='$date')
         |AND isclick=1
       """.stripMargin
    println(sqlRequest)
    val slimUnionlog = spark.sql(sqlRequest)

    // 数据关联
    val rawData = unitidList
      .join(slimUnionlog, Seq("unitid"), "left_outer")
      .select("searchid", "unitid", "industry", "exp_cvr", "date")
    rawData.createOrReplaceTempView("raw_data")

    // 计算两天的数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  industry,
         |  date,
         |  sum(exp_cvr) * 0.0000001 / count(1) as pcvr
         |FROM
         |  raw_data
         |GROUP BY unitid, industry, date
       """.stripMargin
    println(sqlRequest2)
    val data = spark.sql(sqlRequest2)

    val data1 = data.filter(s"`date`='$date1'").withColumn("pcvr1", col("pcvr")).select("unitid", "industry", "pcvr1")
    val data2 = data.filter(s"`date`='$date'").withColumn("pcvr2", col("pcvr")).select("unitid", "industry", "pcvr2")

    val resultDF = data1
      .join(data2, Seq("unitid", "industry"), "outer")
      .select("unitid", "industry", "pcvr1", "pcvr2")

    resultDF

  }

  def getSuggestUnitid(date: String, hour: String, spark: SparkSession) = {
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  industry
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |  `date`='$date1'
         |AND
         |  `hour`='23'
         |AND
         |  version='qtt_demo'
         |AND
         |  is_recommend=1
         |AND
         |  industry in ('feedapp', 'elds')
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest).distinct()

    data
  }



}

