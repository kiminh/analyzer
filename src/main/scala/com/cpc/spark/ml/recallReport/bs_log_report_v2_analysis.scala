package com.cpc.spark.ml.recallReport

import bslog.Bslog.NoticeLogBody
import com.cpc.spark.streaming.tools.Encoding
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object bs_log_report_v2_analysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bs log report")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val date = args(0)

    val sql: String =
      s"""
        |select groups_hit_new_user_ids from dl_cpc.recall_filter_number_report_v2
        |WHERE date='$date' and media_appsid in ('80001098','80001292')
      """.stripMargin
    println(sql)
    val data=spark.sql(sql).withColumn("new_user_ids",explode(split(col("groups_hit_new_user_ids"),",")))
    data.show(10)

    val sql2=
      s"""
         |select unitid,choose from
         |dl_cpc.recall_filter_number_report_v2
         |WHERE date='$date'
       """.stripMargin
    val data2=spark.sql(sql2).join(data,Seq("unitid"),"inner")
    data.repartition(1).write.mode("overwrite").saveAsTable("test.wy00")
  }
}
