package com.cpc.spark.OCPC_elds

import com.cpc.spark.novel.OperateMySQL
import com.cpc.spark.tools.{CalcMetrics, OperateMySQL}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object ocpc_all {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val spark = SparkSession.builder()
      .appName(s"ocpc_all date = $date ")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val Sql1 =
      s"""
         |select
         |a.day,
         |a.type,
         |a.unitid,
         |a.userid,
         |a.searchid,
         |a.conversion_goal,
         |a.price,
         |a.isshow,
         |a.isclick,
         |if(b.searchid is not null,1,0) as iscvr
         |from
         |(select
         |`date` as day,
         |case when industy='feedapp' and is_api_callback = 1 then 'app_call_back'
         |when industy='feedapp' and is_api_callback = 0 then 'not_app_call_back'
         |else industy end as type,
         |unitid,
         |userid,
         |searchid,
         |conversion_goal,
         |price,
         |isshow,
         |isclick
         |from dl_cpc.ocpc_basedata_union_events
         |where `date`='$date'
         |and is_ocpc = 1
         |and ocpc_log_length > 0
         |and media_appsid in ("80000001", "80000002")
         |and isshow=1
         |and adsrc=1
         |and antispam=0
         |and industy in ('elds','feedapp','wzcp') )a
         |left join (
         |    select
         |    searchid,
         |    case when cvr_goal='cvr1' then 1
         |    when cvr_goal = 'cvr2' then  2
         |    when cvr_goal = 'cvr3' then 3 end as conversion_goal
         |     from dl_cpc.ocpc_label_cvr_hourly
         |     where `date`='$date'
         |     group by searchid,case when cvr_goal='cvr1' then 1
         |    when cvr_goal = 'cvr2' then  2
         |    when cvr_goal = 'cvr3' then 3 end
         |  ) b on a.searchid = b.searchid and a.conversion_goal = b.conversion_goal
             """.stripMargin
    println(Sql1)
    val union = spark.sql(Sql1)

    //保存到临时表里
    union.createOrReplaceTempView("union")
    println("union is successful! ")

//    val Sql2 =
//      s"""
//         |select
//         |type,
//         |userid,
//         |unitid,
//         |sum(case when isclick=1 then price else null end)/100 as ocpc_cost,
//         |sum(isshow) as ocpc_show_cnt,
//         |sum(isclick) as ocpc_click_cnt,
//         |sum(iscvr) as ocpc_convert_cnt,
//         |sum(isclick)/sum(isshow) as ctr,
//         |sum(iscvr)/sum(isclick) as post_cvr,
//         |sum(case when isclick=1 then price else null end)/100/sum(isshow)*1000 as cpm,
//         |sum(case when isclick=1 then price else null end)/100 / sum(iscvr) as cpa,
//         |0.0 as ocpc_cost_ratio,
//         |0.0 as post_cvr_ratio,
//         |0 as  ocpc_cost_warning,
//         |0 as post_cvr_warning,
//         |day
//         |from union
//         |group by day,type,userid,unitid
//             """.stripMargin
//    println(Sql2)
//
//    val result1 = spark.sql(Sql2)
//    result1.show(10)
//    result1.repartition(1)
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_all_detail")
//    println("result1 is successful! ")

    val Sql2 =
      s"""
         |select
         |a.type,
         |a.userid,
         |a.unitid,
         |a.ocpc_cost,
         |a.ocpc_show_cnt,
         |a.ocpc_click_cnt,
         |a.ocpc_convert_cnt,
         |a.ctr,
         |a.post_cvr,
         |a.cpm,
         |a.cpa,
         |(a.ocpc_cost/b.ocpc_cost) as ocpc_cost_ratio,
         |(a.post_cvr/b.post_cvr) as post_cvr_ratio,
         |if(a.ocpc_cost/b.ocpc_cost<0.7,1,0) as ocpc_cost_warning,
         |if(a.post_cvr/b.post_cvr<0.7,1,0) as post_cvr_warning,
         |a.day
         |from
         |(select
         |type,
         |userid,
         |unitid,
         |sum(case when isclick=1 then price else null end)/100 as ocpc_cost,
         |sum(isshow) as ocpc_show_cnt,
         |sum(isclick) as ocpc_click_cnt,
         |sum(iscvr) as ocpc_convert_cnt,
         |sum(isclick)/sum(isshow) as ctr,
         |sum(iscvr)/sum(isclick) as post_cvr,
         |sum(case when isclick=1 then price else null end)/100/sum(isshow)*1000 as cpm,
         |sum(case when isclick=1 then price else null end)/100 / sum(iscvr) as cpa,
         |day
         |from union
         |group by day,type,userid,unitid )a
         |left join
         |(select
         |*
         |from dl_cpc.ocpc_all_detail
         |where day = date_sub('$date',1)
         | )b on a.unitid=b.unitid
             """.stripMargin
    println(Sql2)

    val result1 = spark.sql(Sql2)
    result1.show(10)
    result1.repartition(1)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_all_detail")
    println("result1 is successful! ")


//    val Sql3 =
//      s"""
//         |select
//         |type,
//         |count(distinct userid) as userid_cnt,
//         |count(distinct unitid) as unitid_cnt,
//         |sum(ocpc_cost) as ocpc_cost,
//         |sum(ocpc_show_cnt) as ocpc_show_cnt,
//         |sum(ocpc_click_cnt) as ocpc_click_cnt,
//         |sum(ocpc_convert_cnt) as ocpc_convert_cnt,
//         |sum(ocpc_click_cnt) /sum(ocpc_show_cnt) as ctr,
//         |sum(ocpc_convert_cnt)/sum(ocpc_click_cnt) as post_cvr,
//         |sum(ocpc_cost)/sum(ocpc_show_cnt)*1000 as cpm,
//         |sum(ocpc_cost)/sum(ocpc_convert_cnt) as cpa,
//         |0 as only_cost_warning_cnt,
//         |0 as both_warning_cnt,
//         |0 as only_cvr_warning_cnt,
//         |0.0 as ocpc_cost_ratio,
//         |0.0 as post_cvr_ratio,
//         |0 as  ocpc_cost_warning,
//         |0 as post_cvr_warning,
//         |day
//         |from dl_cpc.ocpc_all_detail
//         |where day='$date'
//         |group by day,type
//             """.stripMargin
//    println(Sql3)
//    val  result2= spark.sql(Sql3)
//    result2.show(10)
//    result2.repartition(1)
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_all_total")
//    println("result2 is successful! ")

    val Sql3 =
      s"""
         |select
         |a.type,
         |a.userid_cnt,
         |a.unitid_cnt,
         |a.ocpc_cost,
         |a.ocpc_show_cnt,
         |a.ocpc_click_cnt,
         |a.ocpc_convert_cnt,
         |a.ctr,
         |a.post_cvr,
         |a.cpm,
         |a.cpa,
         |a.only_cost_warning_cnt,
         |a.both_warning_cnt,
         |a.only_cvr_warning_cnt,
         |(a.ocpc_cost/b.ocpc_cost) as ocpc_cost_ratio,
         |(a.post_cvr/b.post_cvr) as post_cvr_ratio,
         |if(a.ocpc_cost/b.ocpc_cost<0.7,1,0) as ocpc_cost_warning,
         |if(a.post_cvr/b.post_cvr<0.7,1,0) as post_cvr_warning,
         |a.day
         |from
         |(select
         |type,
         |count(distinct userid) as userid_cnt,
         |count(distinct unitid) as unitid_cnt,
         |sum(ocpc_cost) as ocpc_cost,
         |sum(ocpc_show_cnt) as ocpc_show_cnt,
         |sum(ocpc_click_cnt) as ocpc_click_cnt,
         |sum(ocpc_convert_cnt) as ocpc_convert_cnt,
         |sum(ocpc_click_cnt) /sum(ocpc_show_cnt) as ctr,
         |sum(ocpc_convert_cnt)/sum(ocpc_click_cnt) as post_cvr,
         |sum(ocpc_cost)/sum(ocpc_show_cnt)*1000 as cpm,
         |sum(ocpc_cost)/sum(ocpc_convert_cnt) as cpa,
         |count(distinct case when ocpc_cost_warning=1 and post_cvr_warning=0 then unitid else null end) as only_cost_warning_cnt,
         |count(distinct case when ocpc_cost_warning=1 and post_cvr_warning=1 then unitid else null end) as both_warning_cnt,
         |count(distinct case when ocpc_cost_warning=0 and post_cvr_warning=1 then unitid else null end) as only_cvr_warning_cnt,
         |day
         |from dl_cpc.ocpc_all_detail
         |where day='$date'
         |group by day,type )a
         |left join
         |(select *
         |from dl_cpc.ocpc_all_total
         |where day = date_sub('$date',1) )b on a.type=b.type
             """.stripMargin
    println(Sql3)
    val  result2= spark.sql(Sql3)
    result2.show(10)
    result2.repartition(1)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_all_total")
    println("result2 is successful! ")

    val Sql4 =
      s"""
         |select
         |n.type,
         |n.userid_cnt,
         |n.unitid_cnt,
         |n.t_ocpc_cost,
         |(n.t_ocpc_cost-l.y_ocpc_cost) as d_ocpc_cost,
         |m.add_unitid_cnt,
         |m.add_ocpc_cost,
         |l.del_unitid_cnt,
         |l.del_ocpc_cost,
         |l.t_both_ocpc_cost,
         |(l.t_both_ocpc_cost-l.y_both_ocpc_cost) as d_both_ocpc_cost,
         |n.day
         |from
         |(select
         | day,
         | type,
         | userid_cnt,
         | unitid_cnt,
         | ocpc_cost as t_ocpc_cost
         |from dl_cpc.ocpc_all_total
         |where day='$date' )n
         |left join
         |(select
         |a.day,
         |a.type,
         |count(distinct a.unitid) as add_unitid_cnt,
         |sum(a.ocpc_cost) as add_ocpc_cost
         |from
         |(select
         |*
         |from dl_cpc.ocpc_all_detail
         |where day='$date' )a
         |left join
         |(select *
         |from dl_cpc.ocpc_all_detail
         |where day=date_sub('$date',1) )b on a.unitid=b.unitid
         |where b.unitid is null
         |group by a.day,a.type )m on n.day=m.day and n.type=m.type
         |left join
         |(select
         |'$date' as day,
         |p.type,
         |sum(p.ocpc_cost) as y_ocpc_cost,
         |sum(case when q.unitid is not null then p.ocpc_cost else null end) as y_both_ocpc_cost,
         |sum(q.ocpc_cost) as t_both_ocpc_cost,
         |sum(case when q.unitid is null then p.ocpc_cost else null end) as del_ocpc_cost,
         |count(distinct p.unitid) as del_unitid_cnt
         |from
         |(select
         |*
         |from dl_cpc.ocpc_all_detail
         |where day=date_sub('$date',1) )p
         |left join
         |(select *
         |from dl_cpc.ocpc_all_detail
         |where day='$date' )q on p.unitid=q.unitid
         |group by '$date',p.type )l on n.day=l.day and n.type=l.type
             """.stripMargin
    println(Sql4)
    val  result3= spark.sql(Sql4)
    result3.show(10)
    result3.repartition(1)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_all_compare_y")
    println("result3 is successful! ")
  }
}
