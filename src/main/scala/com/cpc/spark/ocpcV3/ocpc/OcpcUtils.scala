package com.cpc.spark.ocpcV3.ocpc

import org.apache.spark.sql.SparkSession
import java.util.Properties

import com.typesafe.config.ConfigFactory


object OcpcUtils {
  def getOcpcLogDiv(date: String, hour: String, hourCnt: Int) = {
    val conf = ConfigFactory.load("ocpc")

    val targetK = conf.getDouble("ocpc_all.targetK")
  }
}
