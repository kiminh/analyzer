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
        |`date` as day,
        |identifier as unitid,
        |sum(cost)/100 as yes_ocpc_cost
        |from dl_cpc.ocpc_detail_report_hourly_v4
        |where `date` = '$date'
        |and hour='23'
        |and conversion_goal =3
        |and version = 'qtt_demo'
        |group by identifier,`date`
             """.stripMargin
    println(Sql1)
    val union = spark.sql(Sql1)

    //保存到临时表里
    union.createOrReplaceTempView("union")
    println ("union is successful! ")

    val Sql2 =
      s"""
         |select
         |b.day,
         |b.unitid,
         |b.yes_ocpc_cost
         |from
         |(select
         |unitid
         |from dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |where `date`='$date'
         |and hour = '06'
         |and version = 'qtt_demo'
         |and is_recommend=1
         |group by unitid )a
         |join
         |(select
         |`date` as day,
         |unitid,
         |sum(case when isclick=1 then price else null end)/100 as yes_ocpc_cost
         |from dl_cpc.ocpc_base_unionlog
         |where `date` ='$date'
         |and (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
         |and media_appsid  in ("80000001", "80000002")
         |and isshow=1
         |and antispam = 0
         |and adsrc = 1
         |group by unitid,`date` ) b on a.unitid=b.unitid
         |group by b.unitid,b.yes_ocpc_cost,b.day
             """.stripMargin

    println(Sql2)
    val total = spark.sql(Sql2)
    total.createOrReplaceTempView("total")
    println ("total is successful! ")

    val Sql3 =
      s"""
         |select
         | l.cv_unitid_cnt,
         | m.in_unitid_cnt,
         | round(m.in_unitid_cnt/l.cv_unitid_cnt,4) as unitid_first_ratio,
         | n.yes_unitid_cnt,
         | round(n.yes_unitid_cnt/m.in_unitid_cnt,4) as unitid_second_ratio,
         | round(l.cv_unitid_cost,2) as cv_unitid_cost,
         | round(m.in_unitid_cost,2) as in_unitid_cost,
         | round(m.in_unitid_cost/l.cv_unitid_cost,4) as cost_first_ratio,
         | round(n.yes_ocpc_cost,2) as yes_ocpc_cost,
         | round(n.yes_ocpc_cost/m.in_unitid_cost,4) as cost_second_ratio,
         | m.day
         |from
         |(select
         | p.day,
         |count(distinct p.unitid) as in_unitid_cnt,
         |sum(p.yes_ocpc_cost) as in_unitid_cost
         |from
         |(select
         |a.day,
         |a.unitid,
         |max(a.yes_ocpc_cost) as yes_ocpc_cost
         |from
         |(select *
         |from union
         |union all
         |select *
         |from total )a
         | group by a.day,a.unitid )p
         |group by p.day )m
         |left join
         |(select
         |day,
         |count(distinct unitid) as yes_unitid_cnt,
         |sum(yes_ocpc_cost) as yes_ocpc_cost
         |from union
         |group by day )n on m.day=n.day
         |left join
         |(select
         |day,
         |count(distinct unitid) as cv_unitid_cnt,
         |sum(case when isclick=1 then price else null end)/100 as cv_unitid_cost
         |from dl_cpc.cpc_basedata_union_events
         |where day ='$date'
         |and (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
         |and media_appsid  in ("80000001", "80000002")
         |and isshow=1
         |and adsrc = 1
         |and siteid>0
         |group by day )l on m.day = l.day
             """.stripMargin

    println(Sql3)
    val result = spark.sql(Sql3)
    result.show(10)
    result.repartition(1)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_elds_ld_data")
    println("result is successful! ")

    val tableName1 = "report2.ocpc_elds_ld_data"
    val deleteSql1 = s"delete from $tableName1 where day = '$date' "
    OperateMySQL.update(deleteSql1) //先删除历史数据
    OperateMySQL.insert(result,tableName1) //插入到MySQL中的report2库中
  }
}
