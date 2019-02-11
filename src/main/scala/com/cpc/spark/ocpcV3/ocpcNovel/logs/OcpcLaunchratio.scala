package com.cpc.spark.ocpcV3.ocpcNovel.logs

import org.apache.spark.sql.SparkSession

object OcpcLaunchratio {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0)
    val sql1=
      s"""
         |select
         |  dt,
         |  unitid,
         |  usertype,
         |  adclass,
         |  case
         |  when media_appsid in ('80000001','80000002') then "qtt"
         |  when media_appsid in ('80001098','80001292') then "novel"
         |  ELSE "other" END as media,
         |  sum(case WHEN isclick == 1 then price else 0 end) as money_byunit,
         |  sum(isclick) as isclick_byunit,
         |  sum(isshow) as isshow_byunit
         |from dl_cpc.slim_union_log
         |where dt= $date
         |and isshow = 1
         |and antispam = 0
         |and adsrc = 1
         |AND userid > 0
         |  AND (charge_type IS NULL OR charge_type = 1)
         |group by
         |  dt,
         |  unitid,
         |  usertype,
         |  adclass,
         |  case
         |  when media_appsid in ('80000001','80000002') then "qtt"
         |  when media_appsid in ('80001098','80001292') then "novel"
         |  ELSE "other" END
       """.stripMargin

    spark.sql(sql1).write.mode("overwrite").saveAsTable("test.OcpcLaunchdata")

    val sql2=
      s"""
         |select
         |  unitid,
         |  case when qtt_money = 0 and novel_money > 0 then '1'
         |  else '0' end as choose
         |from
         |(
         |  select
         |  unitid,
         |  sum(money) as sum_money,
         |  sum(if(media=='qtt',money,0)) as qtt_money,
         |  sum(if(media=='novel',money,0)) as novel_money
         |  from test.OcpcLaunchdata
         |) a
       """.stripMargin

//    val data1=spark.sql(sql2)

//      spark.sql("select * from test.OcpcLaunchdata where media = 'qtt'").join(data1,Seq("unitid"))
//        .write.mode("overwrite").saveAsTable("test.OcpcLaunchdata2")

    val sql3=
      s"""
         |select
         |  mode,
         |  round(sum(isclick_byunit)*100 / sum(isshow_byunit),3) as ctr,
         |  round(sum(money_byunit)*10/sum(isshow_byunit),3) as cpm,
         |  round(sum(money_byunit)*10/sum(isclick_byunit),3) as acp
         |from
         |(
         |  select
         |  unitid,
         |  case when length(ext_string["ocpc_log"]) > 0 then 'ocpc'
         |  else 'cpc' end as mode
         |  from dl_cpc.cpc_novel_union_log
         |  WHERE `date` = $date
         |) a
         |join test.OcpcLaunchdata2 b
         |on a.unitid=b.unitid
         |group by mode
       """.stripMargin

//    spark.sql(sql3).write.mode("overwrite").saveAsTable("test.OcpcLaunchdata3")







  }

}
