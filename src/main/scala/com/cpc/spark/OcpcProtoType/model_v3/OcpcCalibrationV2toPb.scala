package com.cpc.spark.OcpcProtoType.model_v3

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import ocpcFactor.ocpcFactor.{OcpcFactorList, SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import ocpcFactor.ocpcFactor.{OcpcFactorList, SingleItem}

import scala.collection.mutable.ListBuffer

object OcpcCalibrationV2toPb {
  def main(args: Array[String]): Unit = {
    /*
    val expTag: Nothing = 1
    val unitid: Nothing = 2
    val ideaid: Nothing = 3
    val slotid: Nothing = 4
    val slottype: Nothing = 5
    val adtype: Nothing = 6
    val cvrCalFactor: Double = 7
    val jfbFactor: Double = 8
    val postCvr: Double = 9
    val highBidFactor: Double = 10
    val lowBidFactor: Double = 11
    计算新版的cvr平滑策略：
    1. 抽取基础数据
    2. 计算该维度下pcoc与计费比、后验cvr等等指标
    3. 计算该维度下根据给定highBidFactor计算出的lowBidFactor
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)

    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val fileName = "test.pb"

    println("parameters:")
    println(s"date=$date, hour=$hour, version:$version")

    // 抽取基础数据
    val unitidList = getOcpcUnit(date, hour, spark)

    val rawData = getCalibrationData(version, date, hour, spark)

    val resultDF = rawData
      .join(unitidList, Seq("unitid", "conversion_goal"), "inner")


    savePbPack(resultDF, fileName, version, date, hour, spark)


  }

  def savePbPack(dataset: DataFrame, filename: String, version: String, date: String, hour: String, spark: SparkSession): Unit = {
    /*
    string expTag = 1;
    int64 unitid = 2;
    int64 ideaid = 3;
    string slotid = 4;
    int64 slottype = 5;
    int64 adtype = 6;
    double cvrCalFactor = 7;
    double jfbFactor = 8;
    double postCvr = 9;
    double highBidFactor = 10;
    double lowBidFactor = 11;
     */
    var list = new ListBuffer[SingleItem]
    dataset.createOrReplaceTempView("raw_data")
    val sqlRequest =
      s"""
         |SELECT
         |  exp_tag,
         |  unitid,
         |  ideaid,
         |  slotid,
         |  slottype,
         |  adtype,
         |  1.0 / pcoc as cvr_cal_factor,
         |  1.0 / jfb as jfb_factor,
         |  post_cvr,
         |  high_bid_factor,
         |  low_bid_factor
         |FROM
         |  raw_data
       """.stripMargin
    println(sqlRequest)
    val resultData = spark
      .sql(sqlRequest)
      .selectExpr("exp_tag", "cast(unitid as bigint) unitid", "cast(ideaid as bigint) ideaid", "cast(slotid as string) slotid", "cast(slottype as bigint) slottype", "cast(adtype as bigint) adtype", "cast(cvr_cal_factor as double) cvr_cal_factor", "cast(jfb_factor as double) jfb_factor", "cast(post_cvr as double) post_cvr", "cast(high_bid_factor as double) high_bid_factor", "cast(low_bid_factor as double) low_bid_factor")

    println("size of the dataframe:")
    println(resultData.count)
    resultData.show(10)
    resultData.printSchema()
    resultData
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit(version))
//        .repartition(10).write.mode("overwrite").saveAsTable("test.check_ocpc_pb_data")
        .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_calibration_v2_pb_hourly")
    var cnt = 0

    for (record <- resultData.collect()) {
      val expTag = record.getAs[String]("exp_tag")
      val unitid = record.getAs[Long]("unitid")
      val ideaid = record.getAs[Long]("ideaid")
      val slotid = record.getAs[String]("slotid")
      val slottype = record.getAs[Long]("slottype")
      val adtype = record.getAs[Long]("adtype")
      val cvrCalFactor = record.getAs[Double]("cvr_cal_factor")
      val jfbFactor = record.getAs[Double]("jfb_factor")
      val postCvr = record.getAs[Double]("post_cvr")
      val highBidFactor = record.getAs[Double]("high_bid_factor")
      val lowBidFactor = record.getAs[Double]("low_bid_factor")

      if (cnt % 100 == 0) {
        println(s"expTag:$expTag, unitid:$unitid, ideaid:$ideaid, slotid:$slotid, slottype:$slottype, adtype:$adtype, cvrCalFactor:$cvrCalFactor, jfbFactor:$jfbFactor, postCvr:$postCvr, highBidFactor:$highBidFactor, lowBidFactor:$lowBidFactor")
      }
      cnt += 1


      cnt += 1
      val currentItem = SingleItem(
        expTag = expTag,
        unitid = unitid,
        ideaid = ideaid,
        slotid = slotid,
        slottype = slottype,
        adtype = adtype,
        cvrCalFactor = cvrCalFactor,
        jfbFactor = jfbFactor,
        postCvr = postCvr,
        highBidFactor = highBidFactor,
        lowBidFactor = lowBidFactor
      )
      list += currentItem

    }

    val result = list.toArray[SingleItem]
    val adRecordList = OcpcFactorList(
      records = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }


  def getCalibrationData(version: String, date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |  *
         |FROM
         |  dl_cpc.ocpc_calibration_v2_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getOcpcUnit(date: String, hour: String, spark: SparkSession) = {
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

    val base = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .select("unitid", "userid", "ideas", "bid", "ocpc_bid", "ocpc_bid_update_time", "conversion_goal", "status")

    base.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    cast(conversion_goal as int) as conversion_goal
         |FROM
         |    base_data
         |WHERE
         |    conversion_goal > 0
       """.stripMargin

    println(sqlRequest)

    val resultDF = spark.sql(sqlRequest)

    resultDF.show(10)
    resultDF


  }

}
