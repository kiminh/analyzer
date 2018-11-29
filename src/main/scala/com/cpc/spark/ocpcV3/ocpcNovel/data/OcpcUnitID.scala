package com.cpc.spark.ocpcV3.ocpcNovel.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcUnitID {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString

    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select unitid, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal from adv.unit where is_ocpc=1 and ideas is not null) as tmp"


    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val base = data.select("unitid", "bid", "ocpc_bid", "ocpc_bid_update_time", "conversion_goal")


    val updateTime = base.withColumn("date", lit(date)).withColumn("hour", lit(hour))

    println("########## updateTime #################")
    updateTime.show(10)

    val tableName = "test.ocpc_unitid_update_time_" + hour
    updateTime.write.mode("overwrite").saveAsTable(tableName)

  }
}