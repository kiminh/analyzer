package com.cpc.spark.novel

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author WangYao
  * @date 2019/03/06
  */
object NovelUnionEvents {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val hour = args(1)
        val spark = SparkSession.builder()
          .appName(s"NovelUnionLog date = $date and hour = $hour")
          .enableHiveSupport()
          .getOrCreate()
        val sql =
            s"""
               |select *
               |from dl_cpc.cpc_basedata_union_events
               |where media_appsid in ('80001098','80001292','80001011','80001539','80002480')
               |      and day= '$date' and hour = '$hour'
             """.stripMargin

        println(sql)

//        spark.sql(sql).write.mode("overwrite").insertInto("dl_cpc.cpc_novel_union_events")
        spark.sql(sql).toDF.coalesce(10)
          .write
          .partitionBy("day", "hour", "minute")
          .mode(SaveMode.Append) // 修改为Append
          .parquet(
            s"""
               |hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_novel_union_events/
         """.stripMargin.trim)

      spark.sql("msck repair table dl_cpc.cpc_novel_union_events")

//      spark.sql(
//        s"""
//           |ALTER TABLE dl_cpc.cpc_basedata_union_events
//           | add if not exists PARTITION(`day` = "$date", `hour` = "$hour")
//           | LOCATION 'hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_basedata_union_events/day=$date/hour=$hour'
//      """.stripMargin.trim)
        println(" -- write cpc_basedata_union_events to hive successfully -- ")
    }
}
