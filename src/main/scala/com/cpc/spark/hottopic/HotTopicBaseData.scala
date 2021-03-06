package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
/**
  * @author Jinbao
  * @date 2019/3/1 19:50
  */
object HotTopicBaseData {
    def main(args: Array[String]): Unit = {

        val day = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"HotTopicBaseData day = $day, hour = $hour")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select *
               |from dl_cpc.cpc_basedata_union_events
               |where day = '$day' and hour = '$hour'
               |and media_appsid in ('80002819')
             """.stripMargin

        val result = spark.sql(sql)
        val tableName = "dl_cpc.cpc_hot_topic_basedata_union_events"
//        result.repartition(1).write.mode("overwrite").insertInto(tableName)
        println(s"insert into $tableName at date = $day, hour = $hour success !")
//

        result.toDF
          .repartition(1)
          .write
          .partitionBy("day", "hour", "minute")
          .mode(SaveMode.Append) // 修改为Append
          .parquet(
            s"""
               |hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_hot_topic_basedata_union_events/
            """.stripMargin.trim)
        println(s"insert into $tableName at date = $day, hour = $hour success !")

//        for (m<- 0 to 59) {
//            val sqlStmt =
//                """
//                  |ALTER TABLE dl_cpc.cpc_hot_topic_basedata_union_events add if not exists PARTITION (day = "%s", hour = "%s", minute = "%02d")  LOCATION
//                  |       '/warehouse/dl_cpc.db/cpc_hot_topic_basedata_union_events/day=%s/hour=%s/minute=%02d'
//                  |
//                """.stripMargin.format(day,hour,m)
//            println(sqlStmt)
//            spark.sql(sqlStmt)
//
//        }
        val sqlStmt =
            """
              |ALTER TABLE dl_cpc.cpc_hot_topic_basedata_union_events add if not exists PARTITION (day = "%s", hour = "%s")  LOCATION
              |       '/warehouse/dl_cpc.db/cpc_hot_topic_basedata_union_events/day=%s/hour=%s'
              |
                """.stripMargin.format(day, hour, day, hour)
        println(sqlStmt)
        spark.sql(sqlStmt)
    }
}
