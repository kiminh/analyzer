package com.cpc.spark.OCPC_elds

import com.cpc.spark.tools.OperateMySQL
import org.apache.spark.sql.SparkSession
import com.cpc.spark.tools.CalcMetrics

object ocpc_elds_ld {
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
         |a.day,
         |a.unitid,
         |a.userid,
         |a.isclick,
         |a.isshow,
         |a.price,
         |a.siteid,
         |a.is_ocpc,
         |a.ocpc_log,
         |if(b.searchid is not null,1,0) as iscvr
         |from
         |(select
         |day,
         |unitid,
         |userid,
         |searchid,
         |isclick,
         |isshow,
         |price,
         |is_ocpc,
         |ocpc_log,
         |siteid
         |from dl_cpc.cpc_basedata_union_events
         |where day ='$date'
         |and (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
         |and media_appsid  in ("80000001", "80000002")
         |and isshow=1
         |and adsrc = 1 )a
         |left join
         |    (
         |        select tmp.searchid
         |        from
         |        (
         |            select
         |                final.searchid as searchid,
         |                 case
         |                    when final.src="elds" and final.label_type=6 then 1
         |                    when final.src="feedapp" and final.label_type in (4, 5) then 1
         |                    when final.src="yysc" and final.label_type=12 then 1
         |                    when final.src="wzcp" and final.label_type in (1, 2, 3) then 1
         |                    when final.src="others" and final.label_type=6 then 1
         |                    else 0
         |                end as isreport
         |            from
         |            (
         |                select
         |                    searchid, media_appsid, uid,
         |                    planid, unitid, ideaid, adclass,
         |                    case
         |                        when (adclass like '134%' or adclass like '107%') then "elds" --二类电商
         |                        when (adslot_type<>7 and adclass like '100%') then "feedapp" --feedapp
         |                        when (adslot_type=7 and adclass like '100%') then "yysc"  --应用商城
         |                        when adclass in (110110100, 125100100) then "wzcp" --网赚
         |                        else "others"
         |                    end as src,
         |                    label_type
         |                from
         |                    dl_cpc.ml_cvr_feature_v1
         |                where
         |                    `date`='$date'
         |                    and label2=1
         |                    and media_appsid in ("80000001", "80000002")
         |                ) final
         |            ) tmp
         |        where tmp.isreport=1 --真正的转化
         |        group by tmp.searchid
         |    ) b
         |    on a.searchid = b.searchid
             """.stripMargin
    println(Sql1)
    val union = spark.sql(Sql1)

    //保存到临时表里
    union.createOrReplaceTempView("union")
    println ("union is successful! ")

    val Sql2 =
      s"""
         |select
         |day,
         |'可获取转化单元' as type,
         |sum(case when isclick=1 then price else null end)/100 as cost,
         |sum(isshow) as show_cnt,
         |sum(isclick) as click_cnt,
         |sum(iscvr) as cvr_cnt,
         |count(distinct case when price>0 then userid else null end) as userid_cnt,
         |count(distinct case when price>0 then unitid else null end) as unitid_cnt
         |from union
         |where siteid>0
         |group by day,'可获取转化单元'
             """.stripMargin

    println(Sql2)
    val result1 = spark.sql(Sql2)
    result1.createOrReplaceTempView("result1")
    println ("result1 is successful! ")

    val Sql3 =
      s"""
         |select
         |q.day,
         |"满足准入单元" as type,
         |sum(case when q.isclick=1 then q.price else null end)/100 as cost,
         |sum(q.isshow) as show_cnt,
         |sum(q.isclick) as click_cnt,
         |sum(q.iscvr) as cvr_cnt,
         |count(distinct case when q.price>0 then q.userid else null end) as userid_cnt,
         |count(distinct case when q.price>0 then q.unitid else null end) as unitid_cnt
         |from
         |(select
         |a.unitid
         |from
         |(select
         |unitid
         |from dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |where `date`='$date'
         |and hour = '06'
         |and version = 'qtt_demo'
         |and is_recommend=1
         |group by unitid
         |UNION
         |select unitid
         |from union
         |where is_ocpc=1 and length(ocpc_log)>0
         |group by unitid )a
         |group by a.unitid)p
         |join
         |(select *
         |from union )q on p.unitid=q.unitid
         |group by q.day,"满足准入单元"
             """.stripMargin

    println(Sql3)

    val result2 = spark.sql(Sql3)
    result2.createOrReplaceTempView("result2")
    println ("result2 is successful! ")


    val Sql4 =
      s"""
         |select
         |"ocpc" as type,
         |sum(case when isclick=1 then price else null end)/100 as ocpc_cost,
         |sum(case when isclick=1 then price else null end)/100 as cost,
         |sum(case when isclick=1 then price else null end)/100/sum(case when isclick=1 then price else null end)/100 as cost_ratio,
         |sum(isshow) as show_cnt,
         |sum(isclick) as click_cnt,
         |sum(iscvr) as cvr_cnt,
         |count(distinct case when price>0 then userid else null end) as userid_cnt,
         |count(distinct case when price>0 then unitid else null end) as unitid_cnt,
         |day
         |from union
         |where is_ocpc=1
         |and length(ocpc_log)>0
         |group by day,"ocpc"
             """.stripMargin

    println(Sql4)
    val result3 = spark.sql(Sql4)
    result3.createOrReplaceTempView("result3")
    println ("result3 is successful! ")


    val Sql5 =
      s"""
         |select
         |a.type,
         |b.ocpc_cost,
         |a.cost,
         |b.ocpc_cost / a.cost as cost_ratio,
         |a.show_cnt,
         |a.click_cnt,
         | a.cvr_cnt,
         |a.userid_cnt,
         |a.unitid_cnt,
         |a.day
         |from
         |(
         | select
         | *
         |  from
         | result2
         |UNION ALL
         |select
         | *
         |from
         |result3
         | ) a
         |left join (
         |select
         | *
         |from
         | result4
         | ) b on a.day = b.day
         |UNION ALL
         |select
         | *
         |from
         | result4
             """.stripMargin

    println(Sql5)
    val result = spark.sql(Sql5)
    result.show(10)
    result.repartition(1)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_elds_ld_data")
    println("result is successful! ")

//    val tableName1 = "report2.ocpc_elds_ld_data"
//    val deleteSql1 = s"delete from $tableName1 where day = '$date' "
//    OperateMySQL.update(deleteSql1) //先删除历史数据
//    OperateMySQL.insert(result,tableName1) //插入到MySQL中的report2库中
  }
}
