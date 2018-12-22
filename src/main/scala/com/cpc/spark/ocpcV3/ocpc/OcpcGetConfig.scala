package com.cpc.spark.ocpcV3.ocpc

import org.apache.spark.sql.SparkSession
import java.util.Properties


object OcpcGetConfig {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()


  }
}

