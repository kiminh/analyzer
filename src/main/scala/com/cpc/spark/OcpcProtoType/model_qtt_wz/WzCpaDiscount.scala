package com.cpc.spark.OcpcProtoType.model_qtt_wz

import java.io.FileOutputStream
import ocpcCpaDiscount.ocpcCpaDiscount.{OcpcCpaDiscountList, SingleItem}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

object WzCpaDiscount {
  def main(args: Array[String]): Unit = {
    /*
    author by wy
    pb文件格式：
    key值这样设计：
    qtt_wz&adslot_type&adtype
    novel_wz&adslot_type&adtype
    66_wz&adslot_type&adtype
    qjp_wz&adslot_type&adtype

    adtype 文本 1 1
    adtype 大图 2 2
    adtype 图文 3 5
    adtype 组图 4 8
    adtype 互动 5 9
    adtype 开屏 6 10
    adtype 横幅 7 11
    adtype 横版视频 8 4
    adtype 激励 9 12
    adtype 竖版视频 10 13
    adslottype 列表页 1 1
    adslottype 详情页 2 2
    adslottype 互动页 3 3
    adslottype 开屏 4 4
    adslottype 横幅 5 14
    adslottype 视频 6 12
    adslottype 激励 7 9
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    Logger.getRootLogger.setLevel(Level.WARN)
    val df = spark.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter",",")  //分隔符，默认为 ,
      .load("hdfs://emr-cluster/user/cpc/wy/wz_cpa_discount.csv")
    df.show()

    val resultDF = df.select($"_c0".alias("media"),$"_c2".alias("adtype"),$"_c4".alias("adslot_type")
      ,$"_c5".alias("discount"))
    savePbPack(resultDF)
  }

  def savePbPack(dataset: DataFrame): Unit = {
    var list = new ListBuffer[SingleItem]
    val filename = "ocpc_cpa_discount.pb"
    println("size of the dataframe")
    println(dataset.count)
    println(s"filename: $filename")
    dataset.show(10)
    dataset.printSchema()
    var cnt = 0

    for (record <- dataset.collect()) {
      val media = record.getAs[String]("media")
      val adslot_type = record.getAs[String]("adslot_type")
      val adtype = record.getAs[String]("adtype")
      val key = media+"_wz&" + adslot_type + "&" + adtype
      val discount = record.getAs[String]("discount").toDouble

      println(s"key: $key,discount: $discount")

      val currentItem = SingleItem(
        key = key,
        discount = discount
      )
      list += currentItem

    }
    val result = list.toArray[SingleItem]
    val adRecordList = OcpcCpaDiscountList(
      records = result
    )

    println("length of the array")
    println(result.length)
    adRecordList.writeTo(new FileOutputStream(filename))
    println("complete save data into protobuffer")

  }
}


