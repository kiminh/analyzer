package com.cpc.spark.OcpcProtoType.data

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object OcpcUnitList {
  def main(args: Array[String]): Unit = {
    /*
    选取cpa_given：从前端adv直接读取cpa_given、更新时间等相关数据，并将identifier作为主键
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // 链接adv数据库
    val unitList = getOcpcUnit(date, hour, spark)
    unitList.write.mode("overwrite").saveAsTable("test.ocpc_unit_list_hourly")
//    unitList
//      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_unit_list_hourly")
  }

  def getOcpcUnit(date: String, hour: String, spark: SparkSession) = {
    val url = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com:3306/adv?useUnicode=true&characterEncoding=utf-8"
    val user = "adv_live_read"
    val passwd = "seJzIPUc7xU"
    val driver = "com.mysql.jdbc.Driver"
    val table = "(select id, user_id, target_medias, bid, ocpc_bid, ocpc_bid_update_time, cast(conversion_goal as char) as conversion_goal, DATE_FORMAT(last_ocpc_opentime, \"%Y-%m-%d\") as ocpc_last_open_date, DATE_FORMAT(last_ocpc_opentime,\"%H\") as ocpc_last_open_hour, status from adv.unit where is_ocpc=1 and ideas is not null) as tmp"

    val data = spark.read.format("jdbc")
      .option("url", url)
      .option("driver", driver)
      .option("user", user)
      .option("password", passwd)
      .option("dbtable", table)
      .load()

    val base = data
      .withColumn("unitid", col("id"))
      .withColumn("userid", col("user_id"))
      .select("unitid", "userid", "target_medias", "bid", "ocpc_bid", "ocpc_bid_update_time", "conversion_goal", "ocpc_last_open_date", "ocpc_last_open_hour", "status")

    base.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    target_medias,
         |    ocpc_bid as cpa_given,
         |    cast(conversion_goal as int) as conversion_goal,
         |    ocpc_bid_update_time as update_timestamp,
         |    from_unixtime(ocpc_bid_update_time) as update_time,
         |    from_unixtime(ocpc_bid_update_time, 'yyyy-MM-dd') as update_date,
         |    from_unixtime(ocpc_bid_update_time, 'HH') as update_hour,
         |    cast(ocpc_last_open_date as string) as ocpc_last_open_date,
         |    cast(ocpc_last_open_hour as string) as ocpc_last_open_hour,
         |    status
         |FROM
         |    base_data
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
