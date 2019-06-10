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
         |where day= '$date' and hour = '$hour' and adtype in (9, 11)
         """.stripMargin

    println(sql)

    spark.sql(sql).toDF
      .write
      .partitionBy("day", "hour")
      .mode(SaveMode.Append) // 修改为Append
      .parquet(
      s"""
         |hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_rewarded_union_events/
         """.stripMargin.trim)

    spark.sql(
      s"""
         |ALTER TABLE dl_cpc.cpc_novel_union_events
         | add if not exists PARTITION(`day` = "$date", `hour` = "$hour")
         | LOCATION 'hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_rewarded_union_events/day=$date/hour=$hour'
          """.stripMargin.trim)
    println(" -- write cpc_rewarded_union_events to hive successfully -- ")
  }

}
