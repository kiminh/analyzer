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
         |select
         |  case
         |    when exptags like '%use_strategy%' then 'A'
         |    else 'B'
         |  end as test_tag,
         |  t1.searchid,
         |  uid,
         |  adclass,
         |  t1.userid,
         |  usertype,
         |  adtype1,
         |  ideaid,
         |  isshow,
         |  isclick,
         |  charge_type,
         |  price,
         |  t2.iscvr,
         |  exp_cvr
         |from
         |  (
         |    select
         |      searchid,
         |      concat_ws(',', exptags) as exptags,
         |      uid,
         |      adclass,
         |      userid,
         |      usertype,
         |      case
         |        when adtype = 2 then 'bigimage'
         |        else 'video'
         |      end as adtype1,
         |      ideaid,
         |      isshow,
         |      isclick,
         |      charge_type,
         |      price,
         |      exp_cvr
         |    from
         |      dl_cpc.cpc_basedata_union_events
         |    where
         |      day = '$date'
         |      and adsrc = 1
         |      --and isclick = 1
         |      --and isshow = 1
         |      and media_appsid in ("80000001")
         |      and adtype in (2, 8, 10) --and    userid>0
         |      and usertype in (0, 1, 2)
         |      and adslot_type = 1
         |      --and (charge_type is NULL or charge_type = 1)
         |      and ideaid > 0
         |      and  interaction=2
         |      and userid > 0
         |      --  and uid not like "%.%"
         |      --  and uid not like "%000000%"
         |      --  and length(uid) in (14, 15, 36)
         |
         |  ) t1
         |  join (  select userid from dl_cpc.cpc_appdown_cvr_threshold  where dt = '$date' group by userid ) tt
         |    on t1.userid = tt.userid
         |  left join (
         |    select
         |      tmp.searchid,
         |      1 as iscvr
         |    from
         |      (
         |        select
         |          final.searchid as searchid,
         |          final.ideaid as ideaid,
         |          case
         |            when final.src = "elds"
         |            and final.label_type = 6 then 1
         |            when final.src = "feedapp"
         |            and final.label_type in (4, 5) then 1
         |            when final.src = "yysc"
         |            and final.label_type = 12 then 1
         |            when final.src = "wzcp"
         |            and final.label_type in (1, 2, 3) then 1
         |            when final.src = "others"
         |            and final.label_type = 6 then 1
         |            else 0
         |          end as isreport
         |        from
         |          (
         |            select
         |              searchid,
         |              media_appsid,
         |              uid,
         |              planid,
         |              unitid,
         |              ideaid,
         |              adclass,
         |              case
         |                when (
         |                  adclass like '134%'
         |                  or adclass like '107%'
         |                ) then "elds" -- 二类电商
         |                when (
         |                  adslot_type <> 7
         |                  and adclass like '100%'
         |                ) then "feedapp"
         |                when (
         |                  adslot_type = 7
         |                  and adclass like '100%'
         |                ) then "yysc" --应用商场
         |                when adclass in (110110100, 125100100) then "wzcp" --网赚彩票（110110100：网赚, 125100100：彩票）
         |                else "others"
         |              end as src,
         |              label_type
         |            from
         |              dl_cpc.ml_cvr_feature_v1
         |            where
         |              `date` = '$date'
         |              and label2 = 1
         |              and media_appsid in ("80000001", "80000002")
         |          ) final
         |      ) tmp
         |    where
         |      tmp.isreport = 1
         |  ) t2 on t1.searchid = t2.searchid
       """.stripMargin

    val baseData = spark.sql(sql1)
    baseData.persist()

    baseData.write.mode("overwrite").saveAsTable("test.baseData_sjq")
    println("========================baseData=======================")
    baseData.show(20)
    println( "baseData has " + baseData.count() + "logs" )

    val pivot_table = baseData
        .select("userid", "adtype1", "ideaid")
      .groupBy("userid", "adtype1" )
      .agg(countDistinct("ideaid").alias("ad_num"))
      .groupBy("userid").pivot("adtype1").agg(sum("ad_num"))
        .na.fill(0, Seq("video", "bigimage"))

    pivot_table.write.mode("overwrite").saveAsTable("test.pivot_table_sjq")

    println("========================pivot_table=====================")
    pivot_table.show(20)

//    pivot_table.write.mode("overwrite").saveAsTable("test.pivot_table_sjq")

    val summary = baseData //同时含视频和大图的数据
        .withColumn("price0", when(col("isclick") === 1, col("price")).otherwise(lit(0)))
        .withColumn("price1", when(col("charge_type") === 2, col("price0")/1000).otherwise( col("price0") ))
        .groupBy("userid", "test_tag", "adtype1", "adclass")
        .agg(sum("isshow").alias("shown"),
          sum("isclick").alias("clickn"),
          sum("iscvr").alias("cvrn"),
          sum("price1").alias("cost")
        ).select("userid", "test_tag", "adtype1", "adclass", "shown", "clickn", "cvrn", "cost")
    println("========================summary=========================")

    summary.persist()

    summary.write.mode("overwrite").saveAsTable("test.summary_sjq")

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
        .filter("adtype1 = 'bigimage'") // 行业大图转化率
      .groupBy("adclass")
      .agg((sum("cvrn")/sum("clickn") ).alias("cvr"))
      .select("adclass", "cvr")

    val userAdclassCvr = userAdclass
      .join( adclassCvr, Seq("adclass"), "inner" )
      .select("userid", "adclass", "cvr" ) //userid为大图userid,

    val uidn_ab = baseData
        //.filter("adtype1 = 'video'")
      .groupBy("test_tag", "adtype1")
      .agg(countDistinct("uid").alias("uidn"))
      .select("test_tag", "adtype1", "uidn")

    val result0 = summary
      // .filter("adtype1 = 'video'")
      .groupBy( "userid","adtype1", "test_tag")
      .agg(
        sum("shown").alias("show_n"),
        sum("clickn").alias("click_n"),
        sum("cvrn").alias("cvr_n"),
        sum("cost").alias("total_cost")
      )
      .withColumn("ctr", col("click_n")*100/col("show_n"))
      .withColumn("cvr", col("cvr_n")*100/col("click_n"))
      .withColumn("cpm", col("total_cost")*10/col("show_n"))
      .withColumn("cpa", col("total_cost")/col("cvr_n")/100)
      .withColumn("acp", col("total_cost")/col("click_n")/100)
      .select("userid","adtype1", "test_tag", "show_n", "ctr", "click_n", "cvr", "cvr_n", "total_cost", "cpm", "cpa", "acp")

    result0.write.mode("overwrite").saveAsTable("test.user_ad_type_sjq0")

    val result = summary
       // .filter("adtype1 = 'video'")
      .groupBy("adtype1", "test_tag" )
      .agg(
          sum("shown").alias("show_n"),
          sum("clickn").alias("click_n"),
          sum("cvrn").alias("cvr_n"),
          sum("cost").alias("total_cost")
           ).join(uidn_ab, Seq("adtype1", "test_tag"), "inner")
      .withColumn("ctr", col("click_n")*100/col("show_n"))
      .withColumn("cvr", col("cvr_n")*100/col("click_n"))
      .withColumn("cpm", col("total_cost")*10/col("show_n"))
      .withColumn("cpa", col("total_cost")/col("cvr_n")/100)
      .withColumn("arpu", col("total_cost")/col("uidn")/100)
      .withColumn("acp", col("total_cost")/col("click_n")/100)
      .select("adtype1", "test_tag", "show_n", "ctr", "click_n", "cvr", "cvr_n", "total_cost", "cpm", "cpa", "arpu", "acp")

    result.write.mode("overwrite").saveAsTable("test.user_ad_type_sjq")


    val userCvr = summary
        .join( pivot_table, Seq("userid"), "left")
      .filter("video > 0")  //排除没有视频的userid
      .groupBy("test_tag", "userid", "adtype1")
      .agg(( sum("cvrn")/sum("clickn") ).alias("cvr"))
      .groupBy("test_tag", "userid").pivot("adtype1").agg(sum("cvr"))
      .select("test_tag", "userid", "video", "bigimage")

    userCvr.write.mode("overwrite").saveAsTable("test.userCvr_sjq")

    val userCvr2 = userCvr
      .join( userAdclassCvr, Seq("userid"), "left" )
      .withColumn("bigimage2", when(col("bigimage").isNull, col("cvr")).otherwise(col("bigimage")))
      .select("test_tag", "userid","adclass", "video", "bigimage","bigimage2")
      .withColumn("flag", when(col("video") > col("bigimage2"), lit(1)).otherwise(lit(0)) )

    userCvr2.write.mode("overwrite").saveAsTable("test.userCvr2_sjq")

    val result2 = userCvr2
      .groupBy("test_tag")
      .agg(
        countDistinct("userid").alias("usern"),
        sum("flag").alias("video_outstand_usern")
      ).withColumn("account", col("video_outstand_usern")/col("usern"))

    result2.write.mode("overwrite").saveAsTable("test.video_outstand_user_account")

  }

}
