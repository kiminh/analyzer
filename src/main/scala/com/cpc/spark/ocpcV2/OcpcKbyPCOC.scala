package com.cpc.spark.ocpcV2

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.ocpc.utils.OcpcUtils._
import org.apache.commons.math3.fitting.{PolynomialCurveFitter, WeightedObservedPoints}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable
import org.apache.spark.sql.functions._
import com.cpc.spark.udfs.Udfs_wj._

object OcpcKbyPCOC {
  /*
  计算k值的新策略：需要jfb和pcoc，时间窗口为3天
  1. 抽取基础数据表，包括searchid, ideaid, precvr, isclick, iscvr1, iscvr2, dynamicbid, price
  2. 按ideaid统计数据：pcoc1, pcoc2, jfb
  3. 生成结果表：k = 0.9 / (pcoc * jfb)
   */

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ocpc v2").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    val hourInt = 72

    println("parameters:")
    println(s"date=$date, hour=$hour, hourInt=$hourInt")
    // 抽取基础数据表，包括searchid, ideaid, precvr, isclick, iscvr1, iscvr2, dynamicbid, price
    val baseData = getBaseData(hourInt, date, hour, spark)

    // 按ideaid统计数据：pcoc1, pcoc2, jfb
    val calcualteResult = calculateData(baseData, date, hour, spark)

    // 生成结果表：k = 0.9 / (pcoc * jfb)
    val result = calcualteResult
      .withColumn("k_ratio2", udfPCOCtoK()(col("pcoc1"), col("jfb")))
      .withColumn("k_ratio3", udfPCOCtoK()(col("pcoc2"), col("jfb")))

    result.write.mode("overwrite").saveAsTable("test.ocpc_check_data20190212")

  }

  def calculateData(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    按ideaid统计数据：pcoc1, pcoc2, jfb
     */
    val data = rawData
      .groupBy("ideaid")
      .agg(
        avg(col("precvr")).alias("precvr"),
        sum(col("isclick")).alias("click"),
        sum(col("iscvr1")).alias("cv1"),
        sum(col("iscvr2")).alias("cv2"),
        sum(col("price")).alias("total_price"),
        sum(col("dynamicbid")).alias("total_bid")
      )
      .withColumn("postcvr1", col("cv1") * 1.0 / col("click"))
      .withColumn("postcvr2", col("cv2") * 1.0 / col("click"))
      .withColumn("jfb", col("total_price") * 1.0 / col("total_bid"))
      .withColumn("pcoc1", col("precvr") * 1.0 / col("postcvr1"))
      .withColumn("pcoc2", col("precvr") * 1.0 / col("postcvr2"))

    val resultDF = data.select("ideaid", "pcoc1", "pcoc2", "jfb")
    resultDF.show(10)
    resultDF
  }


  def getBaseData(hourCnt: Int, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取基础数据表，包括searchid, ideaid, precvr, isclick, iscvr1, iscvr2, dynamicbid, price
     */
    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val newDate = date + " " + hour
    val today = dateConverter.parse(newDate)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.HOUR, -hourCnt)
    val yesterday = calendar.getTime
    val tmpDate = dateConverter.format(yesterday)
    val tmpDateValue = tmpDate.split(" ")
    val date1 = tmpDateValue(0)
    val hour1 = tmpDateValue(1)
    val selectCondition1 = getTimeRangeSql2(date1, hour1, date, hour)
    val selectCondition2 = s"`date` >= '$date1'"

    // 抽取click数据
    val sqlRequest1 =
      s"""
         |SELECT
         |  searchid,
         |  ideaid,
         |  ext['exp_cvr'].int_value * 1.0 / 1000000 as precvr,
         |  isclick,
         |  price,
         |  cast(ocpc_log_dict['dynamicbid'] as double) as dynamicbid
         |FROM
         |  dl_cpc.ocpc_union_log_hourly
         |WHERE
         |  $selectCondition1
         |and media_appsid  in ("80000001", "80000002")
         |and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
         |and isclick = 1
         |and ext['antispam'].int_value = 0
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and ext_int['is_ocpc'] = 1
         |and searchid is not null
       """.stripMargin
    println(sqlRequest1)
    val clickData = spark.sql(sqlRequest1)

    // 抽取cvr1数据
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  label2 as iscvr1
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  $selectCondition2
         |AND
         |  label2=1
         |AND
         |  label_type in (1, 2, 3, 4, 5, 6)
         |GROUP BY searchid, label2
       """.stripMargin
    println(sqlRequest2)
    val cvr1Data = spark.sql(sqlRequest2)

    // 抽取cvr2数据
    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  label as iscvr2
         |FROM
         |  dl_cpc.ml_cvr_feature_v2
         |WHERE
         |  $selectCondition2
         |AND
         |  label=1
         |GROUP BY searchid, label
       """.stripMargin
    println(sqlRequest3)
    val cvr2Data = spark.sql(sqlRequest3)

    // 数据关联
    val resultDF = clickData
      .join(cvr1Data, Seq("searchid"), "left_outer")
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .select("searchid", "ideaid", "precvr", "isclick", "price", "dynamicbid", "iscvr1", "iscvr2")

    resultDF.show(10)
    resultDF
  }




}

