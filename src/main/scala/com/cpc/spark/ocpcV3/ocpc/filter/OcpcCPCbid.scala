package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import com.typesafe.config.ConfigFactory
import ocpcCpcBid.ocpccpcbid.{SingleOcpcMinBid, OcpcMinBidList}
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
    println(s"path is: $expDataPath")
    println(s"fileName is: $fileName")

    val data = getExpData(expDataPath, date, hour, spark)
    data.show(10)

    savePbPack(data, fileName)
  }

  def savePbPack(dataset: DataFrame, filename: String): Unit = {
    var list = new ListBuffer[SingleOcpcMinBid]
    println("size of the dataframe")
    println(dataset.count)
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
      val unit_id = record.getAs[String]("unitid").toInt
      val min_bid = record.getAs[Int]("min_bid")

      println(s"unit_id:$unit_id, min_bid:$min_bid")

      cnt += 1
      val currentItem = SingleOcpcMinBid(
        unitid = unit_id,
        minBid = min_bid
      )
      list += currentItem

    }

    val result = list.toArray[SingleOcpcMinBid]
    val adRecordList = OcpcMinBidList(
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
        min(col("min_bid")).alias("min_bid")
      )
      .select("unitid", "min_bid")

    resultDF

  }

}