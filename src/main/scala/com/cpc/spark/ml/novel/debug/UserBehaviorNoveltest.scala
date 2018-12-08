package com.cpc.spark.ml.novel.debug

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * 统计每天用户点击和展示的广告
  * created time : 2018/10/22 16:25
  *
  * @author zhj
  * @version 1.0
  *
  */
object UserBehaviorNoveltest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0)

    import spark.implicits._

    //click data
    val click_sql =
      s"""
         |select uid, ideaid as click_ideaid, timestamp,
         |      ext['adclass'].int_value as click_adclass,
         |      userid       as click_userid,
         |      planid       as click_planid,
         |      adtype       as click_adtype,
         |      interaction  as click_interaction,
         |      city         as click_city,
         |      adslotid     as click_adslotid,
         |      ext['phone_level'].int_value  as click_phone_level,
         |      ext['brand_title'].string_value  as click_brand_title,
         |      row_number() over(partition by uid,ideaid order by timestamp desc) rn
         |from dl_cpc.cpc_novel_union_log
         |where date='$date'
         |  and isclick = 1 and ideaid > 0
         |  and media_appsid in ("80001098", "80001292")
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |  and uid > 0
      """.stripMargin

    println(click_sql)

    val click_data = spark.sql(click_sql).where("rn = 1")
      .select($"uid",
        $"click_ideaid",
        $"click_adclass",
        $"click_userid",
        $"click_planid",
        $"click_adtype",
        $"click_interaction",
        $"click_city",
        $"click_adslotid",
        $"click_phone_level",
        $"click_brand_title",
        expr("row_number() over (partition by uid order by timestamp desc)").alias("rn"))
      .where("rn <= 5000")
      .persist()

    println(s"click data count $date : " + click_data.count())

    //show data
    val show_sql =
      s"""
         |select uid, ideaid as show_ideaid, timestamp,
         |      ext['adclass'].int_value as show_adclass ,
         |      row_number() over(partition by uid,ideaid order by timestamp desc) rn
         |from dl_cpc.cpc_novel_union_log
         |where date='$date'
         |  and isshow = 1 and ideaid > 0
         |  and media_appsid in ("80001098", "80001292")
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |  and and uid > 0
      """.stripMargin

    println(show_sql)

    spark.sql(show_sql)
      .select($"uid",
        $"show_ideaid",
        $"show_adclass",
        expr("row_number() over (partition by uid order by timestamp desc)").alias("rn"))
      .where("rn <= 5000")
      .coalesce(50)
      .join(click_data, Seq("uid", "rn"), "left")
      .write.mode("overwrite")
      .parquet(s"/warehouse/dl_cpc.db/cpc_user_behaviors_novel_debug/load_date=$date")

    spark.sql(
      s"""
         |alter table dl_cpc.cpc_user_behaviors_novel_debug add partition(load_date='$date')
         |location '/warehouse/dl_cpc.db/cpc_user_behaviors_novel_debug/load_date=$date'
      """.stripMargin)

    //汇总三天数据


  }
}
