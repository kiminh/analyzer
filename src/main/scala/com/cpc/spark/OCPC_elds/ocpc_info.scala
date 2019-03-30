package com.cpc.spark.OCPC_elds


import com.cpc.spark.tools.OperateMySQL
import org.apache.spark.sql.SparkSession
import com.cpc.spark.tools.CalcMetrics


object ocpc_info {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val spark = SparkSession.builder()
      .appName(s"ocpc_elds_ld date = $date ")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val tmpDate = date.replace("-", "")
    val Sql1 =
      s"""
         |select
         |  a.day,
         |  a.industy as industry,
         |  a.searchid,
         |  a.isclick,
         |  a.isshow,
         |  a.price,
         |  a.userid,
         |  a.unitid,
         |  a.adclass,
         |  a.adslot_type,
         |  a.cpagiven,
         |  a.budget,
         |  a.IsHiddenOcpc,
         |  if(b.searchid is not null, 1, 0) as iscvr,
         |  a.conversion_goal
         |from
         |  (
         |    select
         |      `date` as day,
         |      industy,
         |      searchid,
         |      isclick,
         |      isshow,
         |      price,
         |      userid,
         |      unitid,
         |      adclass,
         |      adslot_type,
         |      cast(ocpc_log_dict ['cpagiven'] as double) as cpagiven,
         |      ocpc_log_dict ['budget'] as budget,
         |      ocpc_log_dict ['IsHiddenOcpc'] as IsHiddenOcpc,
         |      conversion_goal
         |    from
         |      dl_cpc.ocpc_basedata_union_events
         |    where
         |      `date` = '$date'
         |      and is_ocpc = 1
         |      and ocpc_log_length > 0
         |  ) a
         |  left join (
         |    select
         |      tmp.searchid
         |    from
         |      (
         |        select
         |          final.searchid as searchid,
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
         |                ) then "elds" --二类电商
         |                when (
         |                  adslot_type <> 7
         |                  and adclass like '100%'
         |                ) then "feedapp" --feedapp
         |                when (
         |                  adslot_type = 7
         |                  and adclass like '100%'
         |                ) then "yysc" --应用商城
         |                when adclass in (110110100, 125100100) then "wzcp" --网赚
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
         |      tmp.isreport = 1 --真正的转化
         |    group by
         |      tmp.searchid
         |  ) b on a.searchid = b.searchid
             """.stripMargin
    println(Sql1)
    val  union= spark.sql(Sql1)

    //保存到临时表里
    union.createOrReplaceTempView("union")
    println ("union is successful! ")

    val Sql2 =
      s"""
         |select
         |t.unitid,
         |t.userid,
         |t.conversion_goal,
         |t.cpa as suggest_CPA
         |from
         |(select
         |  unitid,
         |  userid,
         |  conversion_goal,
         |  cpa,
         |  row_number() over(partition by unitid, userid,conversion_goal order by cost desc) as rank
         | from dl_cpc.ocpc_suggest_cpa_recommend_hourly
         | where `date` = '$date'
         | and `hour`='06'
         | and version = 'qtt_demo') t
         | where t.rank = 1
         | group by t.userid,t.unitid,t.conversion_goal,t.cpa
             """.stripMargin

    println(Sql2)
    val total = spark.sql(Sql2)
    total.createOrReplaceTempView("total")
    println ("total is successful! ")

    val Sql3 =
      s"""
         |select
         |a.userid,
         |a.unitid,
         |a.industry,
         |a.adclass,
         |a.adslot_type,
         |a.conversion_goal,
         |a.ocpc_cost,
         |a.ocpc_yes_cost,
         |a.ocpc_no_cost,
         |c.total_cost,
         |a.ocpc_click_cnt,
         |a.ocpc_show_cnt,
         |a.ocpc_cost/a.ocpc_show_cnt*1000 as ocpc_cpm,
         |a.cpagiven,
         |COALESE(b.cpasuggest,0) as cpasuggest,
         |a.ocpc_cost/a.ocpc_cvr_cnt as cpareal,
         |if((a.ocpc_cost/a.ocpc_cvr_cnt)/a.cpagiven >1.2,0,1) as is_control_cost,
         |a.hidden_budget/a.ocpc_no_cost as hidden_budget_ratio,
         |a.day
         |from
         |(select
         |day,
         |userid,
         |unitid,
         |industry,
         |adclass,
         |adslot_type,
         |conversion_goal,
         |sum(case when isclick=1 then price else null end)/100 as ocpc_cost,
         |sum(case when isclick=1 and IsHiddenOcpc='1' then price else null end)/100 as ocpc_yes_cost,
         |sum(case when isclick=1 and IsHiddenOcpc='0' then price else null end)/100 as ocpc_no_cost,
         |sum(isclick) as ocpc_click_cnt,
         |sum(isshow) as ocpc_show_cnt,
         |sum(case when isclick=1 then cpagiven else null end)/sum(isclick) as cpagive,
         |sum(iscvr) as ocpc_cvr_cnt,
         |sum(case when IsHiddenOcpc='1' then budget else null end)/100 as hidden_budget
         |from union
         |group by day,userid,unitid,industry,adclass )a
         |left join
         |(select *
         |from total )b on a.unitid=b.unitid and a.userid=b.userid and a.conversion_goal=b.conversion_goal
         |left join
         |(select
         |unitid,
         |userid,
         |adslot_type,
         |adclass,
         |conversion_goal,
         |sum(case when isclick=1 then price else null end)/100 as total_cost
         |from dl_cpc.ocpc_basedata_union_events
         |where `date`='$date'
         |group by unitid,userid,adclass,adslot_type,conversion_goal )c on a.unitid=c.unitid and a.userid=c.userid and a.conversion_goal=c.conversion_goal and a.adslot_type=c.adslot_type

             """.stripMargin

    println(Sql3)
    val result1 = spark.sql(Sql3)
    result1.show(10)
    result1.repartition(1)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_basedata_info")
    println("result1 is successful! ")


    val Sql4 =
      s"""
         |select
         |*
         |from
         |(select
         |'总体' as type,
         |sum(ocpc_cost) as ocpc_cost,
         |sum(ocpc_show_cnt) as ocpc_show_cnt,
         |sum(ocpc_click_cnt) as ocpc_click_cnt,
         |sum(ocpc_cvr_cnt) as ocpc_cvr_cnt,
         |sum(ocpc_cost)/sum(ocpc_cvr_cnt)*1000 as cpm,
         |sum(ocpc_yes_cost) as ocpc_yes_cost,
         |sum(ocpc_no_cost) as ocpc_no_cost,
         |count(distinct case when ocpc_cost>0 then userid else null end) as ocpc_userid_cnt,
         |count(distinct case when ocpc_cost>0 then unitid else null end) as ocpc_unitid_cnt,
         |sum(total_cost) as total_cost,
         |sum(ocpc_cost)/sum(total_cost) as ocpc_cost_ratio,
         |sum(case when is_control_cost=1 then ocpc_cost else null end) as ocpc_control_cost,
         |sum(case when is_control_cost=1 then ocpc_cost else null end)/sum(ocpc_cost) as ocpc_control_cost_ratio,
         |count(distinct case when ocpc_cost>0 and is_control_cost=1 then unitid else null end) as ocpc_control_unitid,
         |count(distinct case when ocpc_cost>0 and is_control_cost=1 then unitid else null end)/count(distinct case when ocpc_cost>0 then userid else null end) as ocpc_control_unitid_ratio,
         |day
         |from dl_cpc.ocpc_basedata_info
         |where day='$date'
         |group by day,'总体'
         |UNION ALL
         |select
         |industry as type,
         |sum(ocpc_cost) as ocpc_cost,
         |sum(ocpc_show_cnt) as ocpc_show_cnt,
         |sum(ocpc_click_cnt) as ocpc_click_cnt,
         |sum(ocpc_cvr_cnt) as ocpc_cvr_cnt,
         |sum(ocpc_cost)/sum(ocpc_cvr_cnt)*1000 as cpm,
         |sum(ocpc_yes_cost) as ocpc_yes_cost,
         |sum(ocpc_no_cost) as ocpc_no_cost,
         |count(distinct case when ocpc_cost>0 then userid else null end) as ocpc_userid_cnt,
         |count(distinct case when ocpc_cost>0 then unitid else null end) as ocpc_unitid_cnt,
         |sum(total_cost) as total_cost,
         |sum(ocpc_cost)/sum(total_cost) as ocpc_cost_ratio,
         |sum(case when is_control_cost=1 then ocpc_cost else null end) as ocpc_control_cost,
         |sum(case when is_control_cost=1 then ocpc_cost else null end)/sum(ocpc_cost) as ocpc_control_cost_ratio,
         |count(distinct case when ocpc_cost>0 and is_control_cost=1 then unitid else null end) as ocpc_control_unitid,
         |count(distinct case when ocpc_cost>0 and is_control_cost=1 then unitid else null end)/count(distinct case when ocpc_cost>0 then userid else null end) as ocpc_control_unitid_ratio,
         |day
         |from dl_cpc.ocpc_basedata_info
         |where day='$date'
         |group by day,industry )a

             """.stripMargin

    println(Sql4)
    val result2 = spark.sql(Sql4)
    result2.show(10)
    result2.repartition(1)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_total_info")
    println("result2 is successful! ")

//    val Sql5 =
//      s"""
//         |select
//         |userid,
//         |unitid,
//         |adslot_type,
//         |conversion_goal,
//         |cpagiven,
//         |cpasuggest,
//         |ocpc_cost,
//         |ocpc_show_cnt,
//         |ocpc_click_cnt,
//         |ocpc_cvr_cnt,
//         |ocpc_cpm,
//         |ocpc_yes_cost,
//         |ocpc_no_cost,
//         |cpareal,
//         |is_control_cost,
//         |hidden_budget_ratio,
//         |day
//         |from dl_cpc.ocpc_basedata_info
//         |where day= '$date'
//         |and industry="elds";
//
//             """.stripMargin
//
//    println(Sql5)
//    val result3 = spark.sql(Sql5)
//    result3.show(10)
//    result3.repartition(1)
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_elds_info")
//    println("result3 is successful! ")
//
//    val Sql6 =
//      s"""
//         |select
//         |userid,
//         |unitid,
//         |adslot_type,
//         |conversion_goal,
//         |cpagiven,
//         |cpasuggest,
//         |ocpc_cost,
//         |ocpc_show_cnt,
//         |ocpc_click_cnt,
//         |ocpc_cvr_cnt,
//         |ocpc_cpm,
//         |ocpc_yes_cost,
//         |ocpc_no_cost,
//         |cpareal,
//         |is_control_cost,
//         |hidden_budget_ratio,
//         |day
//         |from dl_cpc.ocpc_basedata_info
//         |where day= '$date'
//         |and industry="feedapp";
//
//             """.stripMargin
//
//    println(Sql6)
//    val result4 = spark.sql(Sql6)
//    result4.show(10)
//    result4.repartition(1)
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_feedapp_info")
//    println("result4 is successful! ")
//
//    val Sql7 =
//      s"""
//         |select
//         |userid,
//         |unitid,
//         |adslot_type,
//         |conversion_goal,
//         |cpagiven,
//         |cpasuggest,
//         |ocpc_cost,
//         |ocpc_show_cnt,
//         |ocpc_click_cnt,
//         |ocpc_cvr_cnt,
//         |ocpc_cpm,
//         |ocpc_yes_cost,
//         |ocpc_no_cost,
//         |cpareal,
//         |is_control_cost,
//         |hidden_budget_ratio,
//         |day
//         |from dl_cpc.ocpc_basedata_info
//         |where day= '$date'
//         |and industry="feedapp";
//
//             """.stripMargin
//
//    println(Sql7)
//    val result5 = spark.sql(Sql7)
//    result5.show(10)
//    result5.repartition(1)
//      .write
//      .mode("overwrite")
//      .insertInto("dl_cpc.ocpc_wzcp_info")
//    println("result5 is successful! ")

//    val tableName1 = "report2.ocpc_elds_ld_data"
//    val deleteSql1 = s"delete from $tableName1 where day = '$date' "
//    OperateMySQL.update(deleteSql1) //先删除历史数据
//    OperateMySQL.insert(result,tableName1) //插入到MySQL中的report2库中
  }
}
