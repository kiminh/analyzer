package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcGetSiteformConversion {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

//    val url = "jdbc:mysql://rm-2zef52mz0p6mv5007.mysql.rds.aliyuncs.com:3306/adv_test?useUnicode=true&characterEncoding=utf-8"
//    val user = "cpcrw"
//    val passwd = "zZdlz9qUB51awT8b"
//    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = s"(select idea_id as ideaid, search_id as searchid, modified_time from adv.site_form_data where DATE(create_time)='$date' and EXTRACT(HOUR FROM create_time)='$hour' and is_show=0) as tmp"
    println(table)


    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()


    val resultDF = data
      .select("ideaid", "searchid")
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))


    resultDF.show(10)

//    resultDF.write.mode("overwrite").saveAsTable("test.site_form_unionlog")
    resultDF
      .repartition(10)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.site_form_unionlog")

  }


}