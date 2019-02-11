package com.cpc.spark.OcpcProtoType.experiment

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcCPAsuggest {
  def main(args: Array[String]): Unit = {
    /*
    identifier维度下的cpa_suggest实验：按照推荐cpa对计算的ecpm进行降权

    ecpm_new = ecpm * (cpa_given / cpa_suggest)^t
    此处，当cpa_given <= 0.75 * cpa_suggest时，t=3；否则t=0
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    val version = args(3).toString

    println("parameters:")
    println(s"date=$date, hour=$hour, version=$version, media=$media")
    //    val data = readExpSet(date, hour, spark)


  }



}

