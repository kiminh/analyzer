package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpcV3.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import ocpcCpcBid.Ocpccpcbid
import ocpcCpcBid.ocpccpcbid.{OcpcCpcBidList, SingleOcpcCpcBid}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer


object OcpcCPCbid {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcMinBid: $date, $hour").enableHiveSupport().getOrCreate()

    val conf = ConfigFactory.load("ocpc")
    val expDataPath = conf.getString("ocpc_all.ocpc_cpcbid.path")
    val fileName = conf.getString("ocpc_all.ocpc_cpcbid.pbfile")
    val smoothDataPath = conf.getString("ocpc_all.ocpc_cpcbid.factor_path")
    println(s"path is: $expDataPath")
    println(s"fileName is: $fileName")
    println(s"path is $smoothDataPath")

    val expData = getExpData(expDataPath, date, hour, spark)
    val cvrData = getCvrData(date, hour, spark)
    val cpmData = getCpmData(date, hour, spark)
    val cvrAlphaData = getCvrAlphaData(smoothDataPath, date, hour, spark)

    val data = expData
        .join(cvrData, Seq("identifier"), "outer")
        .join(cpmData, Seq("identifier"), "outer")
        .join(cvrAlphaData, Seq("identifier"), "left_outer")
        .select("identifier", "min_bid1", "cvr1", "cvr2", "cvr3", "min_bid2", "min_cpm2", "factor1", "factor2", "factor3")
        .withColumn("cvr1", when(col("identifier") === "270", 0.5).otherwise(col("cvr1")))
        .withColumn("cvr2", when(col("identifier") === "270", 0.5).otherwise(col("cvr2")))
        .withColumn("cvr3", when(col("identifier") === "270", 0.5).otherwise(col("cvr3")))
        .withColumn("min_bid", when(col("min_bid1").isNotNull, col("min_bid1")).otherwise(col("min_bid2")))
        .withColumn("min_cpm", col("min_cpm2"))
        .na.fill(0, Seq("min_bid", "cvr1", "cvr2", "cvr3", "min_cpm"))
        .na.fill(0.2, Seq("factor1", "factor2", "factor3"))
        .filter(s"identifier is not ('1854873', '1702796', '1817158', '1875122')")
//        .filter(s"identifier in (1918962, 1921432, 1884679, 1929766)")

    val resultDF = data
        .selectExpr("identifier", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3", "cast(min_cpm as double) as min_cpm", "factor1", "factor2", "factor3")
        .withColumn("date", lit(date))
        .withColumn("hour", lit(hour))
        .withColumn("version", lit("qtt_demo"))
        .repartition(10).write.mode("overwrite").saveAsTable("test.ocpc_post_cvr_unitid_hourly20190218")
//       .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_post_cvr_unitid_hourly")

    savePbPack(data, fileName)
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
    val data = spark
      .table("test.ocpc_pcvr_smooth_cpm")
      .withColumn("min_bid2", col("min_bid"))
      .withColumn("min_cpm2", col("min_cpm"))
      .select("identifier", "min_bid2", "min_cpm2")
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
    val resultData = dataset.selectExpr("identifier", "cast(min_bid as double) min_bid", "cvr1", "cvr2", "cvr3", "cast(min_cpm as double) as min_cpm")
    println(resultData.count)
    resultData.show(10)
    resultData.printSchema()
    var cnt = 0

    for (record <- resultData.collect()) {
      val unit_id = record.getAs[String]("identifier").toLong
      val min_bid = record.getAs[Double]("min_bid")
      val post_cvr1 = record.getAs[Double]("cvr1")
      val post_cvr2 = record.getAs[Double]("cvr2")
      val post_cvr3 = record.getAs[Double]("cvr3")
      val min_cpm = record.getAs[Double]("min_cpm").toLong


      println(s"unit_id:$unit_id, min_bid:$min_bid, post_cvr1:$post_cvr1, post_cvr2:$post_cvr2, post_cvr3:$post_cvr3, min_cpm:$min_cpm")

      cnt += 1
      val currentItem = SingleOcpcCpcBid(
        unitid = unit_id,
        cpcBid = min_bid,
        cvGoal1PostCvr = post_cvr1,
        cvGoal2PostCvr = post_cvr2,
        cvGoal3PostCvr = post_cvr3,
        minCpm = min_cpm
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

  def getExpData(expDataPath: String, date: String, hour: String, spark: SparkSession) = {
    val data = spark.read.format("json").json(expDataPath)

    val resultDF = data
      .groupBy("unitid")
      .agg(
        min(col("min_bid")).alias("min_bid1")
      )
      .withColumn("identifier", col("unitid"))
      .select("identifier", "min_bid1")

    resultDF.show(10)
    resultDF

  }

}