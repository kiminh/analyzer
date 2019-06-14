package com.cpc.spark.OcpcProtoType

import java.io.FileOutputStream

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object OcpcTools {
  def getConversionGoal(date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")

    val url = conf.getString("adv_read_mysql.new_deploy.url")
    val user = conf.getString("adv_read_mysql.new_deploy.user")
    val passwd = conf.getString("adv_read_mysql.new_deploy.password")
    val driver = conf.getString("adv_read_mysql.new_deploy.driver")
    val table = "(select id, user_id, ocpc_bid, cast(conversion_goal as char) as conversion_goal, is_ocpc, ocpc_status from adv.unit where ideas is not null) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val resultDF = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .withColumn("cpagiven", col("ocpc_bid"))
      .selectExpr("unitid",  "userid", "cpagiven", "cast(conversion_goal as int) conversion_goal", "is_ocpc", "ocpc_status")
      .distinct()

    resultDF.show(10)
    resultDF
  }
}