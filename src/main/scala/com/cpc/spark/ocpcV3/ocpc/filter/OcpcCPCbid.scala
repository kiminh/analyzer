package com.cpc.spark.ocpcV3.ocpc.filter

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import ocpcminbidv2.ocpcminbidv2.{BidListV2, SingleBidv2}
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

    val data = getExpData(date, hour, spark)
    data.show(10)
  }

  def getExpData(date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    val expDataPath = conf.getString("ocpc_all.ocpc_cpcbid")

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