package com.cpc.spark.ml.dnn.dssm

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * 统计每天用户点击和展示的广告
  * created time : 2018/10/22 16:25
  *
  * @author qizhi
  * @version 1.0
  *
  */
object UserBehavior {
  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val yesterday = args(0)

    import spark.implicits._

    //click data
    val click_sql =
      s"""
         |select uid, ideaid as click_ideaid, `timestamp`,
         |      adclass as click_adclass,
         |      userid       as click_userid,
         |      planid       as click_planid,
         |      adtype       as click_adtype,
         |      interaction  as click_interaction,
         |      city         as click_city,
         |      adslot_id     as click_adslotid,
         |      phone_level  as click_phone_level,
         |      brand_title  as click_brand_title,
         |      row_number() over(partition by uid,ideaid order by timestamp desc) as rn
         |from dl_cpc.cpc_basedata_union_events
         |where `day`='$yesterday'
         |  and isclick = 1 and ideaid > 0 and adslot_type in (0, 1)
         |  and media_appsid in ("80000001", "80000002")
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |  and length(uid) in (14, 15, 36)
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
        expr("row_number() over (partition by uid order by `timestamp` desc)").alias("rn"))
      .where("rn <= 5000")
      .persist()

    println(s"click data count $yesterday : " + click_data.count())

    //show data
    val show_sql =
      s"""
         |select uid, ideaid as show_ideaid, `timestamp`,
         |      adclass as show_adclass ,
         |      row_number() over(partition by uid,ideaid order by `timestamp` desc) rn
         |from dl_cpc.cpc_basedata_union_events
         |where day='$yesterday'
         |  and isshow = 1 and ideaid > 0 and adslot_type in (0, 1)
         |  and media_appsid in ("80000001", "80000002")
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |  and length(uid) in (14, 15, 36)
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
      .parquet(s"hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_user_behaviors_new/load_date=$yesterday")

    spark.sql(
      s"""
         |alter table dl_cpc.cpc_user_behaviors_new add partition(load_date='$yesterday')
         |location 'hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_user_behaviors_new/load_date=$yesterday'
      """.stripMargin)

    //汇总三天数据


  }
}
