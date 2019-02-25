package com.cpc.spark.OcpcProtoType.experiment

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import ocpc.Ocpc
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import ocpcabtest.ocpcabtest.{OcpcList, SingleRecord}

import scala.collection.mutable.ListBuffer

object OcpcABtestSimple {
  def main(args: Array[String]): Unit = {
    /*
    identifier维度下的ab实验：提供cpcbid和是否开启ab实验的开关。bid的来源来自两个地方：在实验配置文件中提供，根据cpc阶段的历史出价数据进行计算。
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    val conf = ConfigFactory.load("ocpc")
    val expDataPath = conf.getString("ocpc_all.ocpc_abtest.path")
    val fileName = conf.getString("ocpc_all.ocpc_abtest.pbfile")
    println("parameters:")
    println(s"date=$date, hour=$hour")
    println(s"path is: $expDataPath")
    println(s"fileName is: $fileName")

    val data = readExpSet(expDataPath, date, hour, spark)

    savePbPack(data, fileName)
  }

  def savePbPack(dataset: DataFrame, filename: String): Unit = {
    var list = new ListBuffer[SingleRecord]
    println("size of the dataframe")
    val resultData = dataset.select("identifier", "cpc_bid")
    println(resultData.count)
    resultData.show(10)
    resultData.printSchema()
    var cnt = 0

    for (record <- resultData.collect()) {
      val id = record.getAs[String]("identifier")
      val cpc_bid = record.getAs[Double]("cpc_bid")


      println(s"identifier:$id, cpc_bid:$cpc_bid")

      cnt += 1
      val currentItem = SingleRecord(
        identifier = id,
        cpcBid = cpc_bid
      )
      list += currentItem

    }

    val result = list.toArray[SingleRecord]
    val adRecordList = OcpcList(
      adrecord = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")

  }


  def readExpSet(expDataPath: String, date: String, hour: String, spark: SparkSession) = {
    val path = expDataPath

    val data = spark.read.format("json").json(path)

    val resultDF = data
      .groupBy("identifier")
      .agg(
        min(col("cpc_bid")).alias("cpc_bid")
      )
      .selectExpr("cast(identifier as string) identifier", "cast(cpc_bid as double) cpc_bid")

    resultDF
  }

}

