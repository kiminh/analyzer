package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory

import ocpcCpcBid.ocpccpcbid.{OcpcCpcBidList, SingleOcpcCpcBid}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object OcpcCPCbidV2 {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // spark app name
//    scp ocpc_cpc_bid_v2.pb cpc@192.168.80.23:/home/cpc/model_server/data/ocpc_cpc_bid_v2.pb
    val spark = SparkSession.builder().appName(s"OcpcMinBid: $date, $hour").enableHiveSupport().getOrCreate()

    val conf = ConfigFactory.load("ocpc")
    val expDataPath = conf.getString("ocpc_all.ocpc_cpcbid.path_v2")
    val fileName = conf.getString("ocpc_all.ocpc_cpcbid.pbfile_v2")
    val smoothDataPath = conf.getString("ocpc_all.ocpc_cpcbid.factor_path")
    val suggestCpaPath = conf.getString("ocpc_all.ocpc_cpcbid.suggestcpa_path")
    println(s"cpcBid path is: $expDataPath")
    println(s"fileName is: $fileName")
    println(s"smooth factor path is $smoothDataPath")
    println(s"suggest cpa path is $suggestCpaPath")

    val expData = getCpcBidData(expDataPath, date, hour, spark)
    val cvrData = getCvrData(date, hour, spark)
    val cpmData = getCpmData(date, hour, spark)
    val cvrAlphaData = getCvrAlphaData(smoothDataPath, date, hour, spark)
    val suggestCPA = getCPAsuggest(suggestCpaPath, date, hour, spark)

    val data = expData
      .join(cvrData, Seq("identifier"), "outer")
      .join(cpmData, Seq("identifier"), "outer")
      .join(cvrAlphaData, Seq("identifier"), "left_outer")
      .join(suggestCPA, Seq("identifier"), "left_outer")
      .select("identifier", "cpc_bid", "cvr1", "cvr2", "cvr3", "min_bid", "min_cpm", "factor1", "factor2", "factor3", "cpa_suggest", "param_t")
      .withColumn("cvr1", when(col("identifier") === "270", 0.5).otherwise(col("cvr1")))
      .withColumn("cvr2", when(col("identifier") === "270", 0.5).otherwise(col("cvr2")))
      .withColumn("cvr3", when(col("identifier") === "270", 0.5).otherwise(col("cvr3")))
      .na.fill(0, Seq("min_bid", "cvr1", "cvr2", "cvr3", "min_cpm", "cpc_bid", "cpa_suggest", "param_t"))
      .na.fill(0.2, Seq("factor1", "factor2", "factor3"))


    val resultDF = data
      .selectExpr("identifier", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3", "cast(min_cpm as double) as min_cpm", "cast(factor1 as double) factor1", "cast(factor2 as double) as factor2", "cast(factor3 as double) factor3", "cast(cpc_bid as double) cpc_bid", "cpa_suggest", "param_t")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
      .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_post_cvr_unitid_hourly20190226")
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_post_cvr_unitid_hourly")

    savePbPack(data, fileName)
  }

  def getCPAsuggest(suggestCpaPath: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(suggestCpaPath)

    val resultDF = data
      .groupBy("identifier")
      .agg(
        min(col("cpa_suggest")).alias("cpa_suggest"),
        min(col("param_t")).alias("param_t")
      )
      .select("identifier", "cpa_suggest", "param_t")

    resultDF.show(10)
    resultDF
  }

  def getCvrAlphaData(dataPath: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(dataPath)

    val resultDF = data
      .groupBy("unitid")
      .agg(
        min(col("factor1")).alias("factor1"),
        min(col("factor2")).alias("factor2"),
        min(col("factor3")).alias("factor3")
      )
      .withColumn("identifier", col("unitid"))
      .select("identifier", "factor1", "factor2", "factor3")

    resultDF.show(10)
    resultDF
  }

  def getCpmData(date: String, hour: String, spark: SparkSession) = {
//    val data = spark
//        .table("test.ocpc_pcvr_smooth_cpm")
//        .select("identifier", "min_bid", "min_cpm")

    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  0 as min_bid,
         |  0 as min_cpm
         |FROM
         |  test.ocpc_pcvr_smooth_cpm
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)

    data
  }

  def getCvrData(date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date` = '$date' and `hour` = '$hour' and version = 'qtt_demo'"
    val tableName = "dl_cpc.ocpc_pcvr_smooth_hourly"
    val sqlRequest =
      s"""
         |SELECT
         |  identifier,
         |  cvr1,
         |  cvr2,
         |  cvr3
         |FROM
         |  $tableName
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data

  }

  def savePbPack(dataset: DataFrame, filename: String): Unit = {
    var list = new ListBuffer[SingleOcpcCpcBid]
    println("size of the dataframe")
    val resultData = dataset.selectExpr("identifier", "cast(cpc_bid as double) cpc_bid", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3", "cast(min_cpm as double) as min_cpm", "factor1", "factor2", "factor3", "cast(cpa_suggest as double) cpa_suggest", "cast(param_t as double) param_t")
    println(resultData.count)
    resultData.show(10)
    resultData.printSchema()
    var cnt = 0

    for (record <- resultData.collect()) {
      val unit_id = record.getAs[String]("identifier").toLong
      val cpc_bid = record.getAs[Double]("cpc_bid")
      val min_bid = record.getAs[Double]("min_bid")
      val post_cvr1 = record.getAs[Double]("cvr1")
      val post_cvr2 = record.getAs[Double]("cvr2")
      val post_cvr3 = record.getAs[Double]("cvr3")
      val min_cpm = record.getAs[Double]("min_cpm").toLong
      val factor1 = record.getAs[Double]("factor1")
      val factor2 = record.getAs[Double]("factor2")
      val factor3 = record.getAs[Double]("factor3")
      val cpa_suggest = record.getAs[Double]("cpa_suggest")
      val param_t = record.getAs[Double]("param_t")


      if (cnt % 100 == 0) {
        println(s"unit_id:$unit_id, cpc_bid:$cpc_bid, post_cvr1:$post_cvr1, post_cvr2:$post_cvr2, post_cvr3:$post_cvr3, min_cpm:$min_cpm, factor1:$factor1, factor2:$factor2, factor3:$factor3, min_bid:$min_bid, cpa_suggest:$cpa_suggest, param_t:$param_t")
      }
      cnt += 1


      cnt += 1
      val currentItem = SingleOcpcCpcBid(
        unitid = unit_id,
        cpcBid = cpc_bid,
        cvGoal1PostCvr = post_cvr1,
        cvGoal2PostCvr = post_cvr2,
        cvGoal3PostCvr = post_cvr3,
        minCpm = min_cpm,
        cvGoal1Smooth = factor1,
        cvGoal2Smooth = factor2,
        cvGoal3Smooth = factor3,
        minBid = min_bid,
        cpaSuggest = cpa_suggest,
        paramT = param_t
      )
      list += currentItem

    }

    val result = list.toArray[SingleOcpcCpcBid]
    val adRecordList = OcpcCpcBidList(
      adrecord = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }

  def getCpcBidData(expDataPath: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(expDataPath)

    val resultDF = data
      .groupBy("identifier")
      .agg(
        min(col("cpc_bid")).alias("cpc_bid")
      )
      .select("identifier", "cpc_bid")

    resultDF.show(10)
    resultDF

  }

}
