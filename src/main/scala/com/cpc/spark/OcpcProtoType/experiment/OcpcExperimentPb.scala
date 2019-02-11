package com.cpc.spark.OcpcProtoType.experiment

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import ocpcexps.ocpcexps.{OcpcExpList, SingleExpRecord}

import scala.collection.mutable.ListBuffer
import java.io.FileOutputStream

import ocpcexps.Ocpcexps

import scala.collection.mutable

object OcpcExperimentPb {
  def main(args: Array[String]): Unit = {
    /*
    将实验配置数据存储到pb文件中
    按照分隔符抽取expTags，按照实验分区抽取数据
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val expTags = args(3).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, expTags=$expTags")

    // 按照expTags抽取对应实验分区数据
    val data = getExpData(expTags, version, date, spark)
    data.foreach(println)

    // 生成pb文件
    val fileName = "ocpc_test_exp.pb"
    saveDataToPb(data, fileName)


  }

  def saveDataToPb(data: ListBuffer[(String, mutable.LinkedHashMap[String, String])], fileName: String) = {
    var list = new ListBuffer[SingleExpRecord]
    var filename = ""
    val dataList = data.toList
    val cnt = dataList.size

    for (index <- 0 to cnt) {
      val expTag = dataList(index)._1.toString
      val expMap = dataList(index)._2.toMap[String, String]

      println(expTag)

      val currentItem = SingleExpRecord(
        expVersion = expTag,
        expConf = expMap
      )
      list += currentItem

      println(currentItem)

    }
    val result = list.toArray[SingleExpRecord]
    val expRecordList = OcpcExpList(
      exprecord = result
    )

    println("length of the array")
    println(result.length)
    expRecordList.writeTo(new FileOutputStream(filename))

    println("complete save data into protobuffer")
  }

  def getExpData(expTags: String, version: String, date: String, spark: SparkSession) = {
    val expTagList = expTags.split(",")
    var result = new ListBuffer[(String, mutable.LinkedHashMap[String, String])]
    for (expTag <- expTagList) {
      val expData = getExpDataMapByTag(expTag, version, date, spark)
      val currentItem = (expTag, expData)
      result.append(currentItem)
    }
    result
  }

  def getExpDataMapByTag(expTag: String, version: String, date: String, spark: SparkSession) = {
    // 将数据存储到map中
    val selectCondition = s"`date`='$date' and version='$version' and exp_tag='$expTag'"
    println(selectCondition)
    val data = spark
      .table("dl_cpc.ocpc_exp_setting")
      .where(selectCondition)
      .select("map_key", "map_value")

    var dataMap = mutable.LinkedHashMap[String, String]()
    for (row <- data.collect()) {
      val mapKey = row.getAs[String]("map_key")
      val mapValue = row.getAs[String]("map_value")
      dataMap += (mapKey -> mapValue)
    }

    dataMap

  }




}


