package com.cpc.spark.ocpcV3.ocpcNovel.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.ocpc.utils.OcpcUtils.getIdeaUpdates
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType


object OcpcCPAhistoryV2 {
  def main(args: Array[String]): Unit = {
    /*
    选取cpa_history的基本策略：
    1. 抽取基础表
    2. 分别计算该广告单元在趣头条上前一天的历史cpa，在米读小说上前一天的历史cpa以及行业类别的历史cpa
    3. 根据unitid和行业类别关联相关数据
    4. 如果趣头条上至少有一个类别的转化数，给定conversion_goal，如果趣头条上一个类别的转化数都没有，按照米读小说上的转化数给定cpa，如果两类都没有，默认转化目标为1
    5. 按照如下顺序根据转化目标选取合适的cpa：趣头条cpa->米读小说cpa->行业类别cpa
    6. 输出数据
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val baseData = getBaseData(date, hour, spark)
  }

  def getBaseData(date: String, hour: String, spark: SparkSession) = {
    /*
    抽取基础表，只包括前一天在米读小说和趣头条上有记录的unitid和对应adclass
     */
    // 计算日期周期
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val end_date = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(end_date)
    calendar.add(Calendar.DATE, -1)
    val start_date = calendar.getTime
    val date1 = sdf.format(start_date)
    val selectCondition = s"`date`='$date1'"

    // 消费
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  media_appsid,
         |  total_price as total_cost
         |FROM
         |  dl_cpc.ocpcv3_ctr_data_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest1)
    val costData = spark
      .sql(sqlRequest1)
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .groupBy("unitid", "new_adclass", "media_appsid")
      .agg(sum(col("total_cost")).alias("total_cost"))
      .select("unitid", "new_adclass", "media_appsid", "total_cost")

    // 非api回传类转化数(ml_cvr_feature_v1)
    val sqlRequest2 =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  media_appsid,
         |  cvr1_cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr1_data_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvr1Data = spark
      .sql(sqlRequest2)
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .groupBy("unitid", "new_adclass", "media_appsid")
      .agg(sum(col("cvr1_cnt")).alias("cvr1cnt"))
      .select("unitid", "new_adclass", "media_appsid", "cvr1cnt")

    // api回传类转化数(ml_cvr_feature_v2)
    val sqlRequest3 =
      s"""
         |SELECT
         |  unitid,
         |  adclass,
         |  media_appsid,
         |  cvr2_cnt
         |FROM
         |  dl_cpc.ocpcv3_cvr2_data_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest3)
    val cvr2Data = spark
      .sql(sqlRequest3)
      .withColumn("new_adclass", col("adclass")/1000)
      .withColumn("new_adclass", col("new_adclass").cast(IntegerType))
      .groupBy("unitid", "new_adclass", "media_appsid")
      .agg(sum(col("cvr2_cnt")).alias("cvr2cnt"))
      .select("unitid", "new_adclass", "media_appsid", "cvr2cnt")

    // 关联数据构成基础表
    val resultDF = costData
      .join(cvr1Data, Seq("unitid", "new_adclass", "media_appsid"), "left_outer")
      .join(cvr2Data, Seq("unitid", "new_adclass", "media_appsid"), "left_outer")
      .select("unitid", "new_adclass", "media_appsid", "total_cost", "cvr1cnt", "cvr2cnt")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.write.mode("overwrite").saveAsTable("test.ocpcv3_cpa_history_v2_base")
    resultDF

  }

  def getQttCPA(date: String, hour: String, spark: SparkSession) = {
    /*
    抽取趣头条cpa数据
     */


  }
}
