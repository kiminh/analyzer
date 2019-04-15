package com.cpc.spark.novel.toutiao

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.streaming.tools.Gzip.decompress
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import scala.collection.JavaConversions._
import scala.collection.mutable

object toutiaosample {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val spark = SparkSession.builder()
      .appName(s"midu_sample")
      .enableHiveSupport()
      .getOrCreate()
    //穿山甲直投
    val sql =
      s"""
         |select title,concat_ws(' ',collect_set(ButtonText)) ButtonText,concat_ws(' ',collect_set(description)) as description
         |  from
         |  (select title,description,ButtonText,row_number() over(partition by title order by description) rk
         |      from
         |      (select distinct title,description,ButtonText from dl_cpc.cpc_midu_toutiao_log
         |       where `date` > date_add('$date', 30)
         |       ) a
         |   ) b
         |   where rk<10
         |group by title
             """.stripMargin
    println(sql)
    val data = spark.sql(sql)
    data.repartition(1).write.mode("overwrite").saveAsTable("dl_cpc.midu_toutiao_sample")

    //穿山甲dsp
    val sql2 =
      s"""
         |SELECT
         |distinct
         |adid, title
         |FROM dl_cpc.slim_union_log
         |WHERE dt= '$date' and adid != '' and adsrc = 22
         |and media_appsid in ("80001098", "80001292")
         |and title != ''
             """.stripMargin
    println(sql2)
    val data2 = spark.sql(sql2)
    data.repartition(1).write.mode("overwrite").saveAsTable("dl_cpc.midu_toutiao_sample2")
  }
}
