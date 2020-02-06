package com.cpc.spark.oCPX.oCPC.calibration_by_tag.v1

import com.cpc.spark.oCPX.OcpcTools._
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcGetPb_v1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTag = args(3).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version, expTag:$expTag")

    val data = getData(date, hour, version, expTag, spark)

    val expTagSelection = getExpTags(expTag, spark)

    val resultDF = data
      .filter(expTagSelection)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))
      .select("unitid", "conversion_goal", "jfb_factor", "post_cvr", "smooth_factor", "cvr_factor", "high_bid_factor", "low_bid_factor", "cpagiven", "date", "hour", "exp_tag", "is_hidden", "version")
      .cache()
    resultDF.show(10)

    resultDF
      .repartition(1)
//      .write.mode("overwrite").insertInto("test.ocpc_pb_data_hourly")
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_pb_data_hourly")


  }

  def getExpTags(expTag: String, spark: SparkSession) = {
    var editExpTag = expTag
    if (expTag == "base") {
      editExpTag = ""
    }
    val qtt = editExpTag + "Qtt"
    val midu = editExpTag + "MiDu"
    val hottopic = editExpTag + "HT66"
    val others = editExpTag + "Other"
    val result = s"exp_tag in ('$qtt', '$midu', '$hottopic', '$others')"
    result
  }

  def getData(date: String, hour: String, version: String, expTag: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  *,
         |  split(exp_tag, '-') as exp_tag_list
         |FROM
         |  dl_cpc.ocpc_pb_data_hourly_exp
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  exp_tag like '$expTag%'
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("exp_tag", col("exp_tag_list").getItem(0))
      .cache()
    data.show(10)
    data
  }
}


