package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/3/1 19:50
  */
object HotTopicBaseData {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"HotTopicBaseData date = $date, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select *
               |from dl_cpc.cpc_basedata_union_events
               |where day = '$date' and hour = '$hour'
               |and media_appsid in ('80002819')
             """.stripMargin

        val result = spark.sql(sql)
        val tableName = "dl_cpc.cpc_hot_topic_basedata_union_events"
        result.repartition(1).write.mode("overwrite").insertInto(tableName)
        println(s"insert into $tableName at date = $date, hour = $hour success !")
    }
}
