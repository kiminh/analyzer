package com.cpc.spark.ocpc.savepb

import com.cpc.spark.ocpc.utils.OcpcUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object getBaseConversion2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val resultDF = getDataV1(date, hour, spark)
    // TODO: 换成dl_cpc的表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_conversion2_base_hourly")
  }

  def getDataV1(date: String, hour: String, spark: SparkSession) :DataFrame = {
    /**
      * 对conversion_goal=2的广告创意计算转化数
      */

    // 获取使用api回传的广告list
    val cvr3List = spark
      .table("test.ocpc_idea_update_time_" + hour)
      .filter("conversion_goal=2")
      .withColumn("flag", lit(1))
      .select("ideaid", "flag")
      .distinct()

    // 统计这些广告近七天的转化数
    val historyData = getCvr3Data(date, hour, 24 * 7, spark)

    val rawData = historyData
      .groupBy("ideaid", "adclass")
      .agg(sum(col("cvr_cnt")).alias("base_cvr3_cnt"))
      .select("ideaid", "adclass", "base_cvr3_cnt")

    val resultDF = cvr3List
      .join(rawData, Seq("ideaid"), "left_outer")
      .select("ideaid", "adclass", "base_cvr3_cnt")
      .withColumn("cvr3_cnt", when(col("base_cvr3_cnt").isNull, 0).otherwise(col("base_cvr3_cnt")))
      .select("ideaid", "adclass", "cvr3_cnt", "base_cvr3_cnt")

    resultDF
  }


}
