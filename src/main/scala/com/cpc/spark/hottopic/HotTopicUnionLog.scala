package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018/12/21 20:02
  */
object HotTopicUnionLog {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"HotTopicUnionLog date = $date, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()

        val sql =
            s"""
               |select *
               |from dl_cpc.cpc_union_log
               |where `date` = '$date' and hour = '$hour'
               |and media_appsid = '80002819'
             """.stripMargin

        val hotTopic = spark.sql(sql)

        hotTopic.repartition(10)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_hot_topic_union_log")
    }
}
