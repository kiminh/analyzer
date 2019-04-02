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
         |0 as ocpc_cost_warning,
         |0 as post_cvr_warning,
         |day
         |from union
         |group by day,type,userid,unitid
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
         |type,
         |count(distinct userid) as usreid_cnt,
         |count(distinct unitid) as unitid_cnt,
         |sum(ocpc_cost) as ocpc_cost,
         |sum(ocpc_show_cnt) as ocpc_show_cnt,
         |sum(ocpc_click_cnt) as ocpc_click_cnt,
         |sum(ocpc_convert_cnt) as ocpc_convert_cnt,
         |sum(ocpc_cost)/sum(ocpc_show_cnt)*1000 as cpm,
         |sum(ocpc_cost)/sum(ocpc_convert_cnt) as cpa,
         |sum(ocpc_click_cnt) /sum(ocpc_show_cnt) as ctr,
         |sum(ocpc_convert_cnt)/sum(ocpc_click_cnt) as post_cvr,
         |0 as ocpc_cost_warning,
         |0 as post_cvr_warning,
         |day
         |from dl_cpc.ocpc_all_detail
         |where day='$date'
         |group by day,type
             """.stripMargin
    println(Sql3)
    val  result2= spark.sql(Sql3)
    result2.show(10)
    result2.repartition(1)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_all_total")
    println("result2 is successful! ")

//    val Sql4 =
//      s"""
//         |select
//         |day,
//         |type,
//         |count(distinct userid) as usreid_cnt,
//         |count(distinct unitid) as unitid_cnt,
//         |sum(ocpc_cost) as ocpc_cost,
//         |sum(ocpc_show_cnt) as ocpc_show_cnt,
//         |sum(ocpc_click_cnt) as ocpc_click_cnt,
//         |sum(ocpc_convert_cnt) as ocpc_convert_cnt,
//         |sum(ocpc_cost)/sum(ocpc_show_cnt)*1000 as cpm,
//         |sum(ocpc_cost)/sum(ocpc_convert_cnt) as cpa,
//         |sum(ocpc_click_cnt) /sum(ocpc_show_cnt) as ctr,
//         |sum(ocpc_convert_cnt)/sum(ocpc_click_cnt) as post_cvr,
//         |0 as ocpc_cost_warning,
//         |0 as post_cvr_warning
//         |from total
//         |group by day,type,0,0
//             """.stripMargin
//    println(Sql4)
//    val  result3= spark.sql(Sql4)
//    result3.show(10)
//    result3.repartition(1)
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_all_compare_y")
//    println("result3 is successful! ")

  }
}
