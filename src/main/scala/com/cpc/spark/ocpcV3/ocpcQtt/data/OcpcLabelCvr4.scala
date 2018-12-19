package com.cpc.spark.ocpcV3.ocpcQtt.data

import org.apache.spark.sql.SparkSession

object OcpcLabelCvr4 {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString

    val spark = SparkSession.builder().appName(s"ocpc cvr4 label").enableHiveSupport().getOrCreate()
  }
}