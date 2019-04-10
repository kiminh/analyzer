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

    val sql =
      s"""
         |select title,concat_ws(' ',collect_set(ButtonText)) ButtonText,concat_ws(' ',collect_set(description)) as description
         |from
         |(
         |  select title,description,ButtonText,row_number() over(partition by title order by description) rk
         |  from
         |    (select title,description,ButtonText,count(*) as imp from dl_cpc.cpc_midu_toutiao_log
         |    where `date`='$date'
         |    group by title,description,ButtonText) a
         |  where imp>10
         |) b where rk<10
         |group by title
             """.stripMargin

    println(sql)
    val data = spark.sql(sql)
    data.write.mode("overwrite").saveAsTable("test.toutiao_sample")

  }
}
