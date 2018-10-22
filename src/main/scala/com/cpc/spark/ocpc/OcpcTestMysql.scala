package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession

object OcpcTestMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val dataframe_mysql = spark.read.format("jdbc").option("url", "jdbc:mysql://192.168.66.11:3306/adv").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "unit").option("user", "root").option("password", "cpcv587").load()

    val data = dataframe_mysql.select("bid", "ocpc_bid", "ocpc_bid_update_time")

    data.show(10)
  }
}