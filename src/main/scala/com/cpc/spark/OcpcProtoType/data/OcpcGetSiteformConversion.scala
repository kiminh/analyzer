package com.cpc.spark.OcpcProtoType.data

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcGetSiteformConversion {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    val conf = ConfigFactory.load("ocpc")
    
    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.depnew_deployloy.password")
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

    resultDF.write.mode("overwrite").saveAsTable("test.site_form_unionlog")
//    resultDF
//      .repartition(10)
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.site_form_unionlog")

  }


}