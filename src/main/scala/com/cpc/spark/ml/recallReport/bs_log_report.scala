package com.cpc.spark.ml.recallReport

import org.apache.spark.sql.SparkSession

object bs_log_report {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bs log report")
      .enableHiveSupport()
      .getOrCreate()

  }

}
