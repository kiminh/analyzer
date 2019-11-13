package com.cpc.spark.oCPX.deepOcpc.assembly

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object OcpcGetPb {
  /*
  整合整个实验版本下的不同深度转化目标的数据
   */
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag")

    // 计算计费比系数、后验激活转化率、先验点击次留率
    val result = getData(date, hour, version, expTag, spark)

    val resultDF = result
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")


    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_deep_pb_data_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_deep_pb_data_hourly")


  }

  def getData(date: String, hour: String, version: String, expTag: String, spark: SparkSession) = {
    val qttExpTag = expTag + "Qtt"
    val miduExpTag = expTag + "MiDu"
    val hottopicExpTag = expTag + "HT66"
    val selectCondition = s"exp_tag in ('$qttExpTag', '$miduExpTag', '$hottopicExpTag')"
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_deep_pb_data_hourly_exp
         |WHERE
         |  date = '$date'
         |AND
         |  hour = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  $selectCondition
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
  }

}
