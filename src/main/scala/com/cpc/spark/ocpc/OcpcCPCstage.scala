package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcCPCstage {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select * from adv.unit where is_ocpc=1 and ideas is not null) as tmp"


    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val base = data.select("ideas", "bid", "ocpc_bid", "ocpc_bid_update_time")


    val ideaTable = base.withColumn("ideaid", explode(split(col("ideas"), "[,]"))).select("ideaid", "ocpc_bid", "ocpc_bid_update_time")

    ideaTable.createOrReplaceTempView("ideaid_update_time")

    val sqlRequest =
      s"""
         |SELECT
         |    t.ideaid,
         |    t.ocpc_bid as cpa_given,
         |    from_unixtime(t.ocpc_bid_update_time) as update_time,
         |    from_unixtime(t.ocpc_bid_update_time, 'yyyy-MM-dd') as update_date,
         |    from_unixtime(t.ocpc_bid_update_time, 'HH') as update_hour
         |
         |FROM
         |    (SELECT
         |        ideaid,
         |        ocpc_bid,
         |        ocpc_bid_update_time,
         |        row_number() over(partition by ideaid order by ocpc_bid_update_time desc) as seq
         |    FROM
         |        ideaid_update_time) t
         |WHERE
         |    t.seq=1
       """.stripMargin

    println(sqlRequest)

    val updateTime = spark.sql(sqlRequest)

    println("########## updateTime #################")
    updateTime.show(10)

    updateTime.write.mode("overwrite").saveAsTable("test.ocpc_idea_update_time")

  }
}