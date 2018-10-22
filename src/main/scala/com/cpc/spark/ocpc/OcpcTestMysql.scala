package com.cpc.spark.ocpc

import java.util.Properties

import com.github.jurajburian.mailer.Property
import org.apache.spark.sql.SparkSession

object OcpcTestMysql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val url = "jdbc:mysql://192.168.66.11:3306/adv?useUnicode=true&characterEncoding=utf-8"
//    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
//    val url = "jdbc:mysql://192.168.66.11:3306/union?useUnicode=true&characterEncoding=utf-8"
    val user = "root"
    val passwd = "cpcv587"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select bid, ocpc_bid, ocpc_bid_update_time from adv.unit where is_ocpc=1 limit 100) as tmp"


    val mariadbProp = new Properties()
    mariadbProp.put("user", "root")
    mariadbProp.put("password", "cpcv587")
    mariadbProp.put("driver", "com.mysql.jdbc.Driver")

//    val dataframe_mysql = spark.read.format("jdbc").option("url", "jdbc:mysql://192.168.66.11:3306/adv").option("driver", "com.mysql.jdbc.Driver").option("dbtable", "unit").option("user", "root").option("password", "cpcv587").load()

//    val data = spark.read.format("jdbc")
//      .option("url", url)
//      .option("driver", driver)
//      .option("user", user)
//      .option("password", passwd)
//      .option("dbtable", table)
//      .load()
    val data = spark.read.jdbc(url, "adv.unit", mariadbProp)

//    val data = dataframe_mysql.select("bid", "ocpc_bid", "ocpc_bid_update_time")

    data.show(10)
  }
}