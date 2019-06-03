package com.cpc.spark.OcpcProtoType.model_novel_v2

import java.io.FileOutputStream

import ocpcParams.OcpcParams
import ocpcParams.ocpcParams.{OcpcParamsList, SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object OcpcSampleToPb {
  def main(args: Array[String]): Unit = {
    /*
    pb文件格式：
    string expTag = 1;
    string key = 2;
    double cvrCalFactor = 3;
    double jfbFactor = 4;
    double smoothFactor = 5;
    double postCvr = 6;
    double cpaGiven = 7;
    double cpaSuggest = 8;
    double paramT = 9;
    double highBidFactor = 10;
    double lowBidFactor = 11;
    int64 ocpcMincpm = 12;
    int64 ocpcMinbid = 13;
    int64 cpcbid = 14;
	  int64 maxbid = 15;

    key: oCPCNovel&unitid&isHiddenOcpc
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val isHidden = args(3).toInt

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, isHidden:$isHidden")

    val cvrData = getPostCvrAndK(date, hour, version, spark)

    println("NewK")
    println(cvrData.count())
    cvrData.show(10)

    val cvGoal = getConversionGoal(date, hour, spark)
    // 获取postcvr数据

    // 组装数据
    val resultDF = cvrData.join(cvGoal, Seq("identifier", "conversion_goal"), "inner")
      .select("identifier", "kvalue", "conversion_goal", "post_cvr", "cvrcalfactor")
      .withColumn("cpagiven",lit(0.0))
      .withColumn("maxbid",lit(100000))
      .withColumn("smoothfactor", lit(0.5))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit(version))

    resultDF.repartition(1).write.mode("overwrite").insertInto("dl_cpc.ocpc_novel_pb_hourly")
    resultDF.repartition(1).write.mode("overwrite").saveAsTable("dl_cpc.ocpc_novel_pb_once")

  }

  def getConversionGoal(date: String, hour: String, spark: SparkSession) = {
    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select id, user_id, ideas, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal, status from adv.unit where ideas is not null) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .withColumn("cv_flag", lit(1))
      .selectExpr("cast(unitid as string) identifier", "cast(conversion_goal as int) conversion_goal", "cv_flag")
      .distinct()

    resultDF.show(10)
    resultDF
  }

  def getPostCvrAndK(date: String, hour: String, version: String, spark: SparkSession) = {
    /*
    1. 从配置文件和dl_cpc.ocpc_pcoc_jfb_hourly表中抽取需要的jfb数据
    2. 计算新的kvalue
     */
    // 从表中抽取数据
    val sqlRequest =
    s"""
       |SELECT
       |  identifier,
       |  1.0 / pcoc cvrcalfactor,
       |  jfb,
       |  1.0 / jfb as kvalue,
       |  conversion_goal,
       |  post_cvr
       |FROM
       |  dl_cpc.ocpc_pcoc_jfb_hourly
       |WHERE
       |  `date` = '$date' and `hour` = '$hour'
       |AND
       |  version = '$version'
       |AND
       |  jfb > 0
       |AND
       |  pcoc>0
       """.stripMargin

    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF

  }
}


