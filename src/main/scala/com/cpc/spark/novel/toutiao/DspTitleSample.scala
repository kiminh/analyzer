package com.cpc.spark.novel.toutiao

import org.apache.spark.sql.SparkSession

object DspTitleSample {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val spark = SparkSession.builder()
      .appName(s"midu_sample")
      .enableHiveSupport()
      .getOrCreate()
    //穿山甲直投米读
    val sql =
      s"""
         |select title,concat_ws(' ',collect_set(ButtonText)) ButtonText,concat_ws(' ',collect_set(description)) as description
         |  from
         |  (select title,description,ButtonText,row_number() over(partition by title order by description) rk
         |      from
         |      (select distinct title,description,ButtonText from dl_cpc.cpc_midu_toutiao_log
         |       where `date` > date_sub('$date', 30)
         |       ) a
         |   ) b
         |   where rk<10
         |group by title
             """.stripMargin
    println(sql)
    val data = spark.sql(sql)
    data.write.format("com.databricks.spark.csv")
      .option("delimiter","\001").save("/home/cpc/dsp_title/midu_toutiao_sample.csv")
    data.repartition(1).write.mode("overwrite").saveAsTable("dl_cpc.midu_toutiao_sample")

    //穿山甲dsp
    val sql2 =
      s"""
         |SELECT
         |distinct
         |adid, title
         |FROM dl_cpc.slim_union_log
         |WHERE dt> date_sub('$date', 7) and adid != '' and adsrc = 22
         |and media_appsid in ("80001098", "80001292")
         |and title != ''
             """.stripMargin
    println(sql2)
    val data2 = spark.sql(sql2)
    data2.write.format("com.databricks.spark.csv")
      .option("delimiter","\001").save("/home/cpc/dsp_title/dsp_toutiao_sample.csv")
    data2.repartition(1).write.mode("overwrite").saveAsTable("dl_cpc.midu_toutiao_sample2")
  }
}
