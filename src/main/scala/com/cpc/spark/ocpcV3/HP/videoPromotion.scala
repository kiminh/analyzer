package com.cpc.spark.ocpcV3.HP

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import com.cpc.spark.tools.CalcMetrics

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
         |  --t1.searchid,
         |  uid,
         |  cast(adclass/1000 as int) as adclass2,
         |  t1.userid,
         |  case when tt.userid is not NULL then 1 else 0 end as if_use_strategy,
         |  tt.threshold,
         |  --usertype,
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
         |      --and hour = '12'
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
         |  left join (  select userid, expcvr as threshold from dl_cpc.cpc_appdown_cvr_threshold  where dt = '$date' group by userid, expcvr ) tt
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

    val baseData = spark.sql(sql1).na.fill(0,Seq("iscvr"))
    baseData.persist()

    baseData.write.mode("overwrite").saveAsTable("test.baseData_sjq")
    println("========================baseData=======================")
    baseData.show(20)
    println( "baseData has " + baseData.count() + " logs" )

    val pivot_table = baseData
        .filter("if_use_strategy = 1")
        .select("userid", "adtype1", "ideaid")
      .groupBy("userid").pivot("adtype1").agg(countDistinct("ideaid").alias("ad_num"))
      .na.fill(0, Seq("video", "bigimage"))

    pivot_table.write.mode("overwrite").saveAsTable("test.pivot_table_sjq")

    println("========================pivot_table=====================")
    pivot_table.show(20)

//    pivot_table.write.mode("overwrite").saveAsTable("test.pivot_table_sjq")

    val summary = baseData //同时含视频和大图的数据
        .join(pivot_table, Seq("userid"), "left")
        .filter("if_use_strategy = 1 and video > 0")
        .withColumn("price0", when(col("isclick") === 1, col("price")).otherwise(lit(0)))
        .withColumn("price1", when(col("charge_type") === 2, col("price0")/1000).otherwise( col("price0") ))
        .groupBy("userid", "adclass2", "threshold",  "adtype1", "test_tag" )
        .agg(
          sum(col("isshow" )).alias("shown"),
          sum(col("isclick")).alias("clickn"),
          sum(col("iscvr"  )).alias("cvrn"),
          sum(col("isclick")*col("exp_cvr") ).alias("exp_cvr_sum"),
          sum("price1").alias("cost")
        ).select("userid", "adclass2", "threshold",  "adtype1", "test_tag", "shown", "clickn", "cvrn", "exp_cvr_sum","cost")
    println("========================summary=========================")
    summary.persist()
    summary.write.mode("overwrite").saveAsTable("test.summary_sjq")

    val group = summary
      .select("userid", "adtype1", "test_tag").distinct().rdd
      .map(x => (x.getAs[Int]("userid"), x.getAs[String]("adtype1"), x.getAs[String]("test_tag")))
    val groupAuc = addAuc( spark, group, baseData.filter("if_use_strategy = 1") )

    val adclass2Cvr = baseData
      .filter("adtype1 = 'bigimage'")
      .groupBy( "adtype1","adclass2", "test_tag")
      .agg( (sum("iscvr")*100/sum("isclick")).alias("cvr_bigimage_adclass2") )
      .select("adtype1", "adclass2", "test_tag", "cvr_bigimage_adclass2" )

    val result0 = summary
      .join(groupAuc, Seq("userid", "adtype1", "test_tag"), "left")
      .join(adclass2Cvr, Seq( "adtype1", "adclass2", "test_tag"), "left")
      .withColumn("cvr", col("cvrn")*100/col("clickn"))
      .withColumn("cpm", col("cost")*10/col("shown"))
      .withColumn("exp_cvr", col("exp_cvr_sum")*0.0001/col("clickn"))
      .withColumn("pcoc",    col("exp_cvr")/col("cvr"))
      .withColumn("date", lit(date))
      .select("userid", "adclass2", "threshold", "adtype1", "test_tag", "shown", "clickn", "cost", "cvrn", "cvr", "exp_cvr", "pcoc", "auc", "cvr_bigimage_adclass2",  "cpm" , "date")

    //    create table dl_cpc.qtt_shortvideo_cvr_promotion_monitor_summary1
    //    ( userid  int,
    //      adclass2 int,
    //      threshold bigint,
    //      adtype1 string,
    //      test_tag string,
    //      shown int,
    //      clickn int,
    //      cost   double,
    //      cvrn int,
    //      cvr double,
    //      exp_cvr bigint,
    //      pcoc double,
    //      auc double,
    //      cvr_bigimage_adclass2 double,
    //      cpm  double)
    //    comment "group by userid, adclass2, threshold, adtype1, test_tag to summary"
    //    partitioned by (`date` string);
    result0.write.mode("overwrite").insertInto( "dl_cpc.qtt_shortvideo_cvr_promotion_monitor_summary1" )
//    result0.write.mode("overwrite").saveAsTable("test.user_ad_type_sjq0")

    val uidn_ab = baseData
      .filter("if_use_strategy = 1 ")
      .groupBy("test_tag", "adtype1")
      .agg(countDistinct("uid").alias("uidn"))
      .select("test_tag", "adtype1", "uidn")

    val result = summary
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
      .withColumn("date", lit(date))
      .select("adtype1", "test_tag", "show_n", "ctr", "click_n", "cvr", "cvr_n", "total_cost", "cpm", "cpa", "uidn", "arpu", "acp", "date")

    //    create table dl_cpc.qtt_shortvideo_cvr_promotion_monitor_summary2
    //    ( adtype1 string, test_tag string, show_n int, ctr double, click_n int, cvr double, cvr_n int, total_cost double, cpm double, cpa double, uidn int, arpu double, acp double )
    //    comment "group by adtype1, test_tag to summary"
    //    partitioned by (`date` string);

    result.write.mode("overwrite").insertInto("dl_cpc.qtt_shortvideo_cvr_promotion_monitor_summary2")
//    result.write.mode("overwrite").saveAsTable("test.user_ad_type_sjq")

    val userCvr = summary
      .join( pivot_table, Seq("userid"), "left")
      .filter("video > 0")  //排除没有视频的userid
      .groupBy("test_tag", "userid", "adtype1")
      .agg( ( sum("cvrn")/sum("clickn") ).alias("cvr") )
      .groupBy("test_tag", "userid").pivot("adtype1").agg(sum("cvr"))
      .select("test_tag", "userid", "video", "bigimage")

    userCvr.write.mode("overwrite").saveAsTable("test.userCvr_sjq")

    baseData.groupBy("userid", "adclass2")
      .agg(sum("isshow").alias("shown2"))
      .createOrReplaceTempView("baseSummary")

    val sql2 =
      s"""
         |select
         | userid,
         | adclass2
         |from (
         |select
         |  userid,
         |  adclass2,
         |  rank()over(partition by userid, adclass2 order by shown2 desc ) rk
         |from baseSummary
         |) where rk = 1
       """.stripMargin

    val userAdclass = spark.sql(sql2)
    println("=====================userAdclass====================")
    userAdclass.show(10)

    val userAdclassCvr = userAdclass
      .join( adclass2Cvr, Seq("adclass2"), "inner" )
      .select("userid", "adclass2", "test_tag", "cvr_bigimage_adclass2" ) //userid为大图userid,

    val userCvr2 = userCvr
      .join( userAdclassCvr, Seq("userid", "test_tag"), "left" )
      .withColumn("bigimage2", when(col("bigimage").isNull, col("cvr_bigimage_adclass2")).otherwise(col("bigimage")))
      .select("test_tag","userid", "adclass2", "video", "bigimage", "cvr_bigimage_adclass2", "bigimage2")
      .withColumn("flag", when(col("video") > col("bigimage2"), lit(1)).otherwise(lit(0)) )
        .join(summary.filter("adtype1 = 'video'").selectExpr("test_tag", "userid", "clickn as clickn_video",       "cvrn as cvrn_video"),    Seq("test_tag", "userid"), "left")
        .join(summary.filter("adtype1 = 'bigimage'").selectExpr("test_tag", "userid", "clickn as clickn_bigimage", "cvrn as cvrn_bigimage"), Seq("test_tag", "userid"), "left")
      .withColumn( "date", lit(date) )
        .select("test_tag", "userid", "adclass2", "clickn_video", "cvrn_video", "video", "clickn_bigimage", "cvrn_bigimage","bigimage", "cvr_bigimage_adclass2", "bigimage2", "flag", "date")

    //    create table dl_cpc.qtt_shortvideo_cvr_promotion_monitor_summary3
    //    ( test_tag string, userid int, adclass2 int, clickn_video int, cvrn_video int, video double, clickn_bigimage int, cvrn_bigimage int, bigimage double, cvr_bigimage_adclass2 double, bigimage2 double, flag int )
    //    comment "group by test_tag, userid, adclass2 to summary"
    //    partitioned by (`date` string);

    userCvr2.write.mode("overwrite").insertInto("dl_cpc.qtt_shortvideo_cvr_promotion_monitor_summary3")
//    userCvr2.write.mode("overwrite").saveAsTable("test.userCvr2_sjq")

    val result2 = userCvr2
      .groupBy("test_tag")
      .agg(
        countDistinct("userid").alias("usern"),
        sum("flag").alias("video_outstand_usern")
      ).withColumn("account", col("video_outstand_usern")/col("usern"))
        .withColumn("date", lit(date))
        .select( "test_tag", "usern", "video_outstand_usern", "account", "date" )

    //    create table dl_cpc.qtt_shortvideo_cvr_promotion_monitor_good_video_account
    //    ( test_tag string, usern int, video_outstand_usern int,  account double)
    //    comment "users with good video account"
    //    partitioned by (`date` string);

    result2.write.mode("overwrite").insertInto("dl_cpc.qtt_shortvideo_cvr_promotion_monitor_good_video_account")
//    result2.write.mode("overwrite").saveAsTable("test.video_outstand_user_account")

  }

  case class Group ( var userid: Int,
                     val adtype1: String,
                     val test_tag: String,
                     val auc: Double )

  def addAuc( spark: SparkSession, group: RDD[(Int, String, String)], base: DataFrame ) ={
    import spark.implicits._
    val result = scala.collection.mutable.ListBuffer[Group]()
    for (row <- group.collect()){
      val userid = row._1
      val adtype1 = row._2
      val test_tag = row._3
      val df = base
        .filter(s"userid = $userid and adtype1 = '$adtype1' and test_tag = '$test_tag' ")
        .selectExpr("exp_cvr as score", "iscvr as label")
      val auc = CalcMetrics.getAuc(spark, df)
      result += Group(userid, adtype1, test_tag, auc)
    }
    result.toList.toDF()
  }

}
