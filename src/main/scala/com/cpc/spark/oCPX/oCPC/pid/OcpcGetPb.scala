package com.cpc.spark.oCPX.oCPC.pid

import com.cpc.spark.oCPX.OcpcTools.{udfMediaName, udfSetExpTag}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb {
  /*
  从pid程序表中抽取校准数据存入老校准数据流
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

    // 读取当前小时的pid程序计算结果
    val data = readPIDtable(date, hour, version, expTag, spark)

    // 明投单元
    val result = data
      .withColumn("jfb_factor", lit(1.0))
      .withColumn("post_cvr", lit(0.0))
      .withColumn("smooth_factor", lit(0.0))
      .withColumn("cvr_factor", col("current_cali"))
      .withColumn("high_bid_factor", lit(1.0))
      .withColumn("low_bid_factor", lit(1.0))
      .withColumn("cpagiven", lit(1.0))
      .withColumn("is_hidden", lit(0))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    val resultDF = result
      .select("identifier", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")

    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly_alltype")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly_alltype")

  }

  def readPIDtable(date: String, hour: String, version: String, expTag: String,spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  conversion_goal,
         |  media,
         |  current_cali
         |FROM
         |  dl_cpc.ocpc_pid_data_hourly
         |WHERE
         |  date = '$date'
         |AND
         |  hour = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  exp_tag = '$expTag'
       """.stripMargin
    println(sqlRequest)
    val rawData = spark
      .sql(sqlRequest)
      .withColumn("media", udfMediaName()(col("media")))
      .withColumn("exp_tag", udfSetExpTag(expTag)(col("media")))
      .cache()

    rawData.show(10)
    rawData
  }

}


