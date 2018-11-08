package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession

object OcpcGetSiteformConversion {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    val url = "jdbc:mysql://rm-2zef52mz0p6mv5007.mysql.rds.aliyuncs.com:3306/adv_test?useUnicode=true&characterEncoding=utf-8"
    val user = "cpcrw"
    val passwd = "zZdlz9qUB51awT8b"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select idea_id as ideaid, searchid, modified_time from adv_test.site_form_data limit 10) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    data.show(10)




  }


}