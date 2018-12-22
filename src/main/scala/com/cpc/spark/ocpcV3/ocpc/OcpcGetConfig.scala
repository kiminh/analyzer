package com.cpc.spark.ocpcV3.ocpc

import org.apache.spark.sql.SparkSession
import java.util.Properties

import com.typesafe.config.ConfigFactory


object OcpcGetConfig {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString

    val conf = ConfigFactory.load("ocpc")

    val targetK = conf.getDouble("ocpc_all.targetK")
    println(s"the targetK is $targetK")

  }
}

