package com.cpc.spark.ocpcV3.ocpcNovel.result

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._


object OcpcGetK {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // TODO 测试
    val result = getK(date, hour, spark)
    result.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_k_hourly")
  }

  def getK(date: String, hour: String, spark: SparkSession) = {
    val tableName = "test.ocpc_v3_novel_k_regression"
    val rawData = spark
      .table(tableName)
      .where(s"`date`='$date' and `hour`='$hour'")
    rawData.show(10)

    val resultDF = rawData
      .select("unitid")
      .distinct()
      .withColumn("kvalue", lit(0.5))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF
  }
}
