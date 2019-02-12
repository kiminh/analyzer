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
         |where dt= '$date'
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
    println(sql1)
//    spark.sql(sql1).write.mode("overwrite").saveAsTable("test.OcpcLaunchdata")
    //标记直投暗投
    val sql2=
      s"""
         |select
         |  unitid,
         |  qtt_money,
         |  novel_money,
         |  if(qtt_money = 0 and novel_money > 0,1,0) as choose
         |from
         |(
         |  select
         |  unitid,
         |  sum(money_byunit) as sum_money,
         |  sum(if(media=='qtt',money_byunit,0)) as qtt_money,
         |  sum(if(media=='novel',money_byunit,0)) as novel_money
         |  from test.OcpcLaunchdata
         |  group by unitid
         |) a
       """.stripMargin

    val data1=spark.sql(sql2)

      spark.sql("select * from test.OcpcLaunchdata where media = 'novel'").join(data1,Seq("unitid"))
        .write.mode("overwrite").saveAsTable("test.OcpcLaunchdata2")

    val sql3=
      s"""
         |select
         |  choose,
         |  case when length(ext_string["ocpc_log"]) > 0 then 'ocpc'
         |  else 'cpc' end as mode,
         |  sum(case WHEN isclick == 1 then price else 0 end) as money,
         |  round(sum(isclick)*100 / sum(isshow),3) as ctr,
         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isclick),3) as acp
         |from
         |(
         |  select *
         |  from dl_cpc.cpc_novel_union_log
         |  WHERE `date` = '$date'
         |  and isshow = 1
         |  and ext['antispam'].int_value = 0
         |  and adsrc = 1
         |  and media_appsid in ("80001098","80001292")
         |  AND userid > 0
         |  AND (ext["charge_type"] IS NULL
         |       OR ext["charge_type"].int_value = 1)
         |) a
         |left join test.OcpcLaunchdata2 b
         |on a.unitid=b.unitid
         |group by
         |  choose,
         |  case when length(ext_string["ocpc_log"]) > 0 then 'ocpc'
         |  else 'cpc' end
       """.stripMargin

//    spark.sql(sql3).write.mode("overwrite").saveAsTable("test.OcpcLaunchdata3")
    //用户类型：其他非企正企
  val sql4=
    s"""
       |select
       |  choose,
       |  ext['usertype'].int_value as usertype,
       |  sum(case WHEN isclick == 1 then price else 0 end) as money,
       |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
       |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isclick),3) as acp,
       |  round(sum(isclick)*100 / sum(isshow),3) as ctr
       |from
       |(
       |  select *
       |  from dl_cpc.cpc_novel_union_log
       |  WHERE `date` = '$date'
       |  and isshow = 1
       |  and ext['antispam'].int_value = 0
       |  and adsrc = 1
       |  and media_appsid in ("80001098","80001292")
       |  AND userid > 0
       |  AND (ext["charge_type"] IS NULL
       |       OR ext["charge_type"].int_value = 1)
       |) a
       |left join test.OcpcLaunchdata2 b
       |on a.unitid=b.unitid
       |group by
       |  choose,
       |  ext['usertype'].int_value
         """.stripMargin

//        spark.sql(sql4).write.mode("overwrite").saveAsTable("test.OcpcLaunchdata4")

    //用户类型：重点行业
    val sql5=
      s"""
         |select
         |  choose,
         |  case when round(ext['adclass'].int_value/1000000) == 100 then 'app'
         |  when round(ext['adclass'].int_value/1000) == 110110 then 'wz'
         |  else 'notag' end as adclass,
         |  sum(case WHEN isclick == 1 then price else 0 end) as money,
         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isclick),3) as acp,
         |  round(sum(isclick)*100 / sum(isshow),3) as ctr
         |from
         |(
         |  select *
         |  from dl_cpc.cpc_novel_union_log
         |  WHERE `date` = '$date'
         |  and isshow = 1
         |  and ext['antispam'].int_value = 0
         |  and adsrc = 1
         |  and media_appsid in ("80001098","80001292")
         |  AND userid > 0
         |  AND (ext["charge_type"] IS NULL
         |       OR ext["charge_type"].int_value = 1)
         |) a
         |left join test.OcpcLaunchdata2 b
         |on a.unitid=b.unitid
         |group by
         |  choose,
         |  case when round(ext['adclass'].int_value/1000000) == 100 then 'app'
         |  when round(ext['adclass'].int_value/1000) == 110110 then 'wz'
         |  else 'notag' end
         """.stripMargin

//    spark.sql(sql5).write.mode("overwrite").saveAsTable("test.OcpcLaunchdata5")
//    val sql6=
//      s"""
//         |select
//         |  choose,
//         |  case when length(ext_string["ocpc_log"]) > 0 then 'ocpc'
//         |  else 'cpc' end as mode,
//         |  sum(case WHEN isclick == 1 then price else 0 end) as money,
//         |  round(sum(isclick)*100 / sum(isshow),3) as ctr,
//         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
//         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isclick),3) as acp
//         |from
//         |(
//         |  select *
//         |  from test.OcpcLaunchdata2
//         |  WHERE `date` = '$date'
//         |  and isshow = 1
//         |  and ext['antispam'].int_value = 0
//         |  and adsrc = 1
//         |  and media_appsid in ("80001098","80001292")
//         |  AND userid > 0
//         |  AND (ext["charge_type"] IS NULL
//         |       OR ext["charge_type"].int_value = 1)
//         |) a
//         |left join test.OcpcLaunchdata2 b
//         |on a.unitid=b.unitid
//         |group by
//         |  choose,
//         |  case when length(ext_string["ocpc_log"]) > 0 then 'ocpc'
//         |  else 'cpc' end
//           """.stripMargin



  }

}
