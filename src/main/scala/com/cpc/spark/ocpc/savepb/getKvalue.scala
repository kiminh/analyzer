package com.cpc.spark.ocpc.savepb

import com.cpc.spark.ocpc.utils.OcpcUtils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object getKvalue {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val resultDF = getUpdatedK(date, hour, spark)
    // TODO: 换成dl_cpc的表
    resultDF.write.mode("overwrite").saveAsTable("test.ocpc_kvalue_base_hourly")

  }

  def getUpdatedK(date: String, hour: String, spark: SparkSession) :DataFrame = {
    val kValue = spark
      .table("test.ocpc_k_value_table")
      .withColumn("raw_k_value", when(col("k_value").isNull, 0.694).otherwise(col("k_value")))
      .select("ideaid", "adclass", "raw_k_value")
    val regressionK = getRegressionK(date, hour, spark)

    val resultDF = kValue
      .join(regressionK, Seq("ideaid"), "left_outer")
      .select("ideaid", "adclass", "raw_k_value", "regression_k_value")
      .withColumn("k_value", when(col("flag").isNotNull && col("regression_k_value")>0, col("regression_k_value")).otherwise(col("raw_k_value")))

    resultDF


  }

}