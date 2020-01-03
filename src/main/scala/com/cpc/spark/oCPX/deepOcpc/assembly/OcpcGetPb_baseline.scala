package com.cpc.spark.oCPX.deepOcpc.assembly

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcGetPb_baseline {
  /*
  整合整个实验版本下的不同深度转化目标的数据
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString

    println("parameters:")
    println(s"date=$date, hour=$hour")

    // 计算计费比系数、后验激活转化率、先验点击次留率
    val result = getData(date, hour, spark)

    val resultDF = result
      .select("conversion_goal", "exp_tag", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour")


    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_deep_pb_data_hourly_baseline")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_pb_data_hourly_baseline")


  }

  def getData(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  *,
         |  split(exp_tag, '-') as exp_tag_list
         |FROM
         |  dl_cpc.ocpc_deep_pb_data_hourly_baseline_exp
         |WHERE
         |  date = '$date'
         |AND
         |  hour = '$hour'
         |""".stripMargin
    println(sqlRequest)
    val data = spark
        .sql(sqlRequest)
        .withColumn("exp_tag_old", col("exp_tag"))
        .withColumn("exp_tag", col("exp_tag_list").getItem(0))
    data
  }

}
