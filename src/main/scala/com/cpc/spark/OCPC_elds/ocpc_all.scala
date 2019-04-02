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
         |a.type,
         |count(distinct case when a.unitid is not null then a.unitid else null end) as unitid_cnt,
         |count(distinct case when a.userid is not null then a.userid else null end) as userid_cnt,
         |count(distinct case when b.day is null then a.unitid else null end) as add_unitid_cnt,
         |count(distinct case when a.day is null then b.unitid else null end) as del_unitid_cnt,
         |sum(a.ocpc_cost) as t_ocpc_cost,
         |(sum(a.ocpc_cost)-sum(b.ocpc_cost)) as d_ocpc_cost,
         |a.day
         |from
         |(select
         |*
         |from dl_cpc.ocpc_all_detail
         |where day='$date' )a
         |full outer join
         |(select *
         |from dl_cpc.ocpc_all_detail
         |where day=date_sub('$date',1) )b on a.unitid=b.unitid
         |group by a.type,a.day
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
