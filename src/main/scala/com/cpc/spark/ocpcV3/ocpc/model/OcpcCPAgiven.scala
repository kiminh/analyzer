package com.cpc.spark.ocpcV3.ocpc.model

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils._
import com.cpc.spark.ocpc.utils.OcpcUtils.getIdeaUpdates
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory


object OcpcCPAgiven {
  def main(args: Array[String]): Unit = {
    /*
    选取cpa_given：从前端adv直接读取cpa_given、更新时间等相关数据，并将identifier作为主键
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // 链接adv数据库
    val cpaGiven = getCPAgiven(date, hour, spark)
    cpaGiven.write.mode("overwrite").saveAsTable("test.ocpc_cpa_given_hourly")
  }

  def getCPAgiven(date: String, hour: String, spark: SparkSession) = {
    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select id, ideas, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal from adv.unit where is_ocpc=1 and ideas is not null) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val base = data
      .withColumn("unitid", col("id"))
      .select("unitid", "ideas", "bid", "ocpc_bid", "ocpc_bid_update_time", "conversion_goal")


    val ideaTable = base.withColumn("ideaid", explode(split(col("ideas"), "[,]"))).select("unitid", "ideaid", "ocpc_bid", "ocpc_bid_update_time", "conversion_goal")

    ideaTable.createOrReplaceTempView("ideaid_update_time")

    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    ideaid,
         |    ocpc_bid as cpa_given,
         |    cast(conversion_goal as int) as conversion_goal,
         |    update_timestamp,
         |    from_unixtime(t.ocpc_bid_update_time) as update_time,
         |    from_unixtime(t.ocpc_bid_update_time, 'yyyy-MM-dd') as update_date,
         |    from_unixtime(t.ocpc_bid_update_time, 'HH') as update_hour
         |FROM
         |    ideaid_update_time
       """.stripMargin

    println(sqlRequest)

    val rawData = spark.sql(sqlRequest)
    val resultDF = rawData
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

    resultDF.show(10)
    resultDF


  }
}
