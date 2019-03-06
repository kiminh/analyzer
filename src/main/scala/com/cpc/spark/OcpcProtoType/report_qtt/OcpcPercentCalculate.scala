package com.cpc.spark.OcpcProtoType.report_qtt

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// 通过zerobid_percent, bottom_halfbid_percent, top_halfbid_percent,
// largebid_percent计算percent
object OcpcPercentCalculate {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val alphas = parseCoefficient("conf")
    val (alpha1, alpha2, alpha3) = alphas
    val spark = SparkSession.builder().appName("calculatePercent").enableHiveSupport().getOrCreate()
    calculatePercent(alpha1, alpha2, alpha3, date, spark)
  }

  // 解析conf文件，获得bottom、top、large的系数，其中zero的系数为0
  def parseCoefficient(fileName: String) = {
    val conf = ConfigFactory.load(s"$fileName")
    val alpha1 = conf.getDouble("predict_alpha.alpha1")
    val alpha2 = conf.getDouble("predict_alpha.alpha2")
    val alpha3 = conf.getDouble("predict_alpha.alpha3")
    (alpha1, alpha2, alpha3)
  }

  // 计算percent并存到分区表中
  def calculatePercent(alpha1: Double, alpha2: Double, alpha3: Double, date: String, spark: SparkSession): Unit ={
    val sqlRequest =
      s"""
        |SELECT
        |	unitid,
        |	userid,
        |	is_recommend,
        |	ocpc_flag,
        |	acb,
        |	kvalue,
        |	cal_bid,
        |	zerobid_percent,
        |	bottom_halfbid_percent,
        |	top_halfbid_percent,
        |	largebid_percent,
        |	($alpha1*bottom_halfbid_percent + $alpha2*top_halfbid_percent + $alpha3*largebid_percent) as predict_percent
        |FROM
        |	dl_cpc.ocpc_suggest_cpa_recommend_hourly
        |WHERE
        | `date` = $date
      """.stripMargin
    val dataDF = spark.sql(sqlRequest)
    dataDF
      .withColumn("date", lit(date))
      .withColumn("version", lit("qtt_demo"))
      .repartition(5).write.mode("overwrite")
      .insertInto("dl_cpc.ocpc_predict_percent")
  }
}
