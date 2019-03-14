package com.cpc.spark.ocpcV3.HP

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._


object videoPromotion {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().appName("videoPromotion").enableHiveSupport().getOrCreate()

    val date = args(0).toString

    val sql1 =
      s"""
         |  select
         |   case
         |     when exptags like '%use_strategy%' then 'A'
         |     else 'B'
         |    end as test_tag,
         |   t1.searchid,
         |   uid,
         |   adclass,
         |   userid,
         |   case
         |     when adtype = 2 then 'bigimage'
         |     else 'video'
         |    end as adtype1,
         |   ideaid,
         |   isshow,
         |   isclick,
         |   charge_type,
         |   price,
         |   t2.iscvr,
         |   exp_cvr
         |   from (
         |    select *
         |    from dl_cpc.slim_union_log
         |where dt = '$date'
         |  and adtype in (2, 8, 10)
         |  and media_appsid = '80000001'
         |  and adslot_type = 1 --列表页
         |  and adsrc = 1
         |  and userid >0
         |  and isshow = 1
         |  --and antispam = 0
         |  and (charge_type is NULL or charge_type = 1)
         |  and interaction=2 --下载
         |  and uid not like "%.%"
         |  and uid not like "%000000%"
         |  and length(uid) in (14, 15, 36)
         |  and ideaid > 0
         |      	) t1
         |   left join (
         |   	   select
         |        searchid,
         |        label2 as iscvr --是否转化
         |       from dl_cpc.ml_cvr_feature_v1
         |      WHERE `date` = '$date'
         |      ) t2
         |   on t1.searchid = t2.searchid
       """.stripMargin

    val baseData = spark.sql(sql1)
    baseData.persist()
    println("========================baseData=======================")
    baseData.show(20)

    val pivot_table = baseData
        .select("userid", "adtype1", "ideaid")
      .groupBy("userid", "adtype1" )
      .agg(countDistinct("ideaid").alias("ad_num"))
      .groupBy("userid").pivot("adtype1").agg(sum("ad_num"))
        .na.fill(0, Seq("video", "bigimage"))
    println("========================pivot_table=====================")
    pivot_table.show(20)

//    pivot_table.write.mode("overwrite").saveAsTable("test.pivot_table_sjq")

    val summary = baseData
        .join(pivot_table, Seq("userid"), "left")
        .filter("video > 0") //去掉没有视频的userid
        .withColumn("price1", when(col("isclick") === 1, col("price")).otherwise(lit(0)))
        .groupBy("userid", "test_tag", "adtype1", "adclass")
        .agg(sum("isshow").alias("shown"),
          sum("isclick").alias("clickn"),
          sum("iscvr").alias("cvrn"),
          sum("price1").alias("cost")
        ).select("userid", "test_tag", "adtype1", "adclass", "shown", "clickn", "cvrn", "cost")
    println("========================summary=========================")
    summary.show(20)

    summary.groupBy("userid", "adclass")
      .agg(sum("shown").alias("shown2"))
      .createOrReplaceTempView("baseSummary")

    val sql2 =
      s"""
         |select
         | userid,
         | adclass
         |from (
         |select
         |  userid,
         |  adclass,
         |  rank()over(partition by userid, adclass order by shown2 desc ) rk
         |from baseSummary
         |) where rk = 1
       """.stripMargin

    val userAdclass = spark.sql(sql2)
    println("=====================userAdclass====================")
    userAdclass.show(10)

    val adclassCvr = summary
        .filter("adtype1 = 'bigimage'")
      .groupBy("adclass")
      .agg((sum("cvrn")/sum("clickn") ).alias("cvr"))
      .select("adclass", "cvr")

    val userAdclassCvr = userAdclass
      .join( adclassCvr, Seq("adclass"), "inner" )
      .select("userid", "adclass", "cvr" )

    val uidn_ab = baseData
        .filter("adtype1 = 'video'")
      .groupBy("test_tag")
      .agg(countDistinct("uid").alias("uidn"))
      .select("test_tag", "uidn")

    val result = summary
        .filter("adtype1 = 'video'")
      .groupBy("test_tag")
      .agg(
          sum("shown").alias("show_n"),
          sum("clickn").alias("click_n"),
          sum("cvrn").alias("cvr_n"),
          sum("cost").alias("total_cost")
           ).join(uidn_ab, Seq("test_tag"), "inner")
      .withColumn("ctr", col("click_n")*100/col("show_n"))
      .withColumn("cvr", col("cvr_n")*100/col("click_n"))
      .withColumn("cpm", col("total_cost")*10/col("show_n"))
      .withColumn("cpa", col("total_cost")/col("cvr_n")/100)
      .withColumn("arpu", col("total_cost")/col("uidn")/100)
      .withColumn("acp", col("total_cost")/col("click_n")/100)
      .select("test_tag", "show_n", "ctr", "click_n", "cvr", "cvr_n", "total_cost", "cpm", "cpa", "arpu", "acp")

    result.write.mode("overwrite").saveAsTable("test.user_ad_type_sjq")


    val userCvr = summary
      .groupBy("test_tag", "userid", "adtype1")
      .agg(( sum("cvrn")/sum("clickn") ).alias("cvr"))
      .groupBy("test_tag", "userid").pivot("adtype1").agg(sum("cvr"))

//    userCvr.write.mode("overwrite").saveAsTable("test.userCvr_sjq")

    val userCvr2 = userCvr
      .join( userAdclassCvr, Seq("userid"), "left" )
      .withColumn("bigimage2", when(col("bigimage").isNull, col("cvr")).otherwise(col("bigimage")))
      .select("test_tag", "userid", "video", "bigimage","bigimage2")
      .withColumn("flag", when(col("video") > col("bigimage2"), lit(1)).otherwise(lit(0)) )


    userCvr2.show()

    val result2 = userCvr2
      .groupBy("test_tag")
      .agg(
        count("userid").alias("usern"),
        sum("flag").alias("video_outstand_usern")
      ).withColumn("account", col("video_outstand_usern")/col("usern"))

    result2.write.mode("overwrite").saveAsTable("test.video_outstand_user_account")

  }

}
