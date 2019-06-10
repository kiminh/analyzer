package com.cpc.spark.rewarded

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * author: huazhenhao
  * date: 6/10/19
  */
object BaseEvent {
  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0)
    val hour = args(1)
    val spark = SparkSession.builder()
      .appName(s"Rewarded Ads UnionLog date = $date and hour = $hour")
      .enableHiveSupport()
      .getOrCreate()
    val sql =
      s"""
         |select *
         |from dl_cpc.cpc_basedata_union_events
         |where day= '$date' and hour = '$hour' and adtype = 11
         """.stripMargin

    println(sql)

    spark.sql(sql).toDF
      .write
      .partitionBy("day", "hour", "minute")
      .mode(SaveMode.Append) // 修改为Append
      .parquet(
      s"""
         |hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_rewarded_union_events/
         """.stripMargin.trim)

    var i = 0
    while (i < 60) {
      var minute = (i / 10).toString
      minute = minute + i % 10
      println(minute)
      spark.sql(
        s"""
           |ALTER TABLE dl_cpc.cpc_rewarded_union_events
           | add if not exists PARTITION(`day` = "$date", `hour` = "$hour", `minute`="$minute")
           | LOCATION 'hdfs://emr-cluster2/warehouse/dl_cpc.db/cpc_rewarded_union_events/day=$date/hour=$hour/minute=$minute'
          """.stripMargin.trim)
      i = i + 1
    }
    println(" -- write cpc_rewarded_union_events to hive successfully -- ")
  }

}
