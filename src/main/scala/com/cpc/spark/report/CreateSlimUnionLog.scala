package com.cpc.spark.report

import org.apache.spark.sql.SparkSession

/**
  * author: huazhenhao
  * date: 11/30/18
  */
object CreateSlimUnionLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("slim-unionlog").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString

    val df = spark.sql(
      s"""
        | select
        |   searchid,
        |   timestamp as ts,
        |   uid,
        |   ext_int['raw_ctr'] as exp_ctr,
        |   ideaid,
        |   bid,
        |   price,
        |   userid,
        |   ext['adclass'].int_value as adclass,
        |   isclick,
        |   isshow,
        |   exptags,
        |   adslot_type,
        |   ext_string['bs_rank_tag'] as bs_rank_tag,
        |   ext_int['embeddingNum'] as embeddingNum,
        |   ext_int["dsp_adnum_by_src_1"] as bs_num
        | from dl_cpc.cpc_union_log
        | where `date`='$date' and hour='$hour'
        | and media_appsid in ('80000001', '80000002')
        | and ext['antispam'].int_value = 0
        | and isshow = 1 and ext['antispam'].int_value = 0
        | and ideaid > 0 and adsrc = 1 and userid > 0
      """.stripMargin)
      df.repartition(10).createOrReplaceTempView("tbl")

    spark.sql(
      s"""
        |insert into dl_cpc.slim_unionlog partition (dt='$date', hour='$hour')
        |select * from tbl
      """.stripMargin)
  }
}
