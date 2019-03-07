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
          .appName(s"NovelUnionLog date = $date")
          .enableHiveSupport()
          .getOrCreate()
        val sql =
            s"""
               |select *
               |from dl_cpc.cpc_basedata_union_events
               |where media_appsid in ('80001098','80001292')
               |      and day= '$date' and hour = '$hour'
             """.stripMargin

        println(sql)

        spark.sql(sql).write.mode("overwrite").insertInto("dl_cpc.cpc_novel_union_events")



    }
}
