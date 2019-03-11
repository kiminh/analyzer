package com.cpc.spark.OcpcProtoType.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcConversionV2 {
  def main(args: Array[String]): Unit = {
    /*
    针对网赚类统计转化数据
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val conversionGoal = args(2).toString

    val result = getLabel(conversionGoal, date, hour, spark)
    result
//      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_label_cvr_hourly")
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_label_cvr_hourly")
    println("successfully save data into table: dl_cpc.ocpc_label_cvr_hourly")
  }

  def getLabel(conversionGoal: String, date: String, hour: String, spark: SparkSession) = {
    /*
    针对于现目前主要行业转化取数逻辑如下：
    网赚：label_type = 1、2
    http://km.qutoutiao.net/pages/viewpage.action?pageId=63406646
     */
    var selectCondition = s"`date`='$date' and hour = '$hour'"

    val sqlRequest =
      s"""
         |SELECT
         |  searchid,
         |  label2 as label
         |FROM
         |  dl_cpc.ml_cvr_feature_v1
         |WHERE
         |  $selectCondition
         |AND
         |  label2=1
         |AND
         |  label_type in (1, 2)
         |GROUP BY searchid, label2
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark
        .sql(sqlRequest)
        .select("searchid", "label")
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("cvr_goal", lit(conversionGoal))

    resultDF.show(10)
    resultDF.printSchema()

    resultDF


  }
}
