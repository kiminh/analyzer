package com.cpc.spark.report

import org.apache.spark.sql.{SaveMode, SparkSession}

object Antispam_TKID {
  def main(args: Array[String]): Unit = {
    val date_before3hours=args(0)
    val hour_before3hours=args(1)
    val date_before4hours=args(2)
    val hour_before4hours=args(3)
    val date_before5hours=args(4)
    val hour_before5hours=args(5)

    val spark=SparkSession.builder()
      .appName("antispam tkid")
      .enableHiveSupport()
      .getOrCreate()

    val sql1=
      s"""
         |select tkid
         |from (
         |     select tkid
         |           ,count(1) as ad_cnt
         |           ,sum(case when isshow=1 then 1 else 0 end) as show_cnt
         |           ,sum(case when isshow=0 then 1 else 0 end) as noshow_cnt
         |     from dl_cpc.cpc_basedata_union_events
         |     where day="${date_before3hours}" and hour="${hour_before3hours}" and media_appsid in (80000001,80000002)
         |     group by tkid having count(1)>10
         |     ) a
         |left join (
         |          SELECT distinct tk
         |          from bdm.qukan_log_v3
         |          where day="${date_before3hours}" and hour="${hour_before3hours}"
         |          ) b on b.tk=a.tkid
         |where show_cnt=0 and b.tk is null
         |order by ad_cnt desc
       """.stripMargin

    val sql2=
      s"""
         |select distinct c.tkid
         |from (
         |     select tkid
         |           ,count(1) as cnt
         |     from dl_cpc.cpc_basedata_union_events
         |     where day = "${date_before5hours}" and hour="${hour_before5hours}"
         |     group by tkid having count(1)>=200
         |     )a
         |inner join (
         |          select tkid
         |                ,count(1) as cnt
         |          from dl_cpc.cpc_basedata_union_events
         |          where day = "${date_before4hours}" and hour="${hour_before4hours}"
         |          group by tkid having count(1)>=200
         |          )b on b.tkid=a.tkid
         |inner join (
         |          select tkid
         |                ,count(1) as cnt
         |          from dl_cpc.cpc_basedata_union_events
         |          where day = "${date_before3hours}" and hour="${hour_before3hours}"
         |          group by tkid having count(1)>=200
         |          )c on c.tkid=a.tkid
       """.stripMargin

    println("sql1: "+sql1)
    println("sql2: "+sql2)

    val res1=spark.sql(sql1)
    val res2=spark.sql(sql2)

    val tkid=res1.union(res2)
      .distinct()
      .select("tkid")
      .repartition(1)
      .write
      .mode(SaveMode.Overwrite)
      .text("/user/cpc/model_server/data/$date_before3hours/$hour_before3hours")

    println("antispam tkid done")


  }
}
