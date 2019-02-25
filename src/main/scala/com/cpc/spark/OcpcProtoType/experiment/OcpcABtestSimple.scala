package com.cpc.spark.OcpcProtoType.experiment

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcABtestSimple {
  def main(args: Array[String]): Unit = {
    /*
    identifier维度下的ab实验：提供cpcbid和是否开启ab实验的开关。bid的来源来自两个地方：在实验配置文件中提供，根据cpc阶段的历史出价数据进行计算。
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    val version = args(3).toString
    val expTag = "ab_test"

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media, expTag=$expTag")
    val data = readExpSet(date, hour, spark)


  }


  def readExpSet(date: String, hour: String, spark: SparkSession) = {
    val path = s"/user/cpc/wangjun/ocpc_exp/ocpc_ab.json"

    val data = spark.read.format("json").json(path)

    val resultDF = data
      .groupBy("identifier")
      .agg(
        min(col("cpc_bid")).alias("cpc_bid")
      )
      .select("identifier", "cpc_bid")

    resultDF
  }

}

