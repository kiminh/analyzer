package com.cpc.spark.ocpcV3.ocpcNovel.logs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.tools.OperateMySQL

object OcpcLaunchReport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0)
    //监控小说ocpc成本控制
//    val sql =
//      s"""
//         |select
//         |case when cpa_ratio<0.8 then '<0.8'
//         |when cpa_ratio>1.2 then '>1.2'
//         |else 'ok' end as tag,count(*),sum(cost/100) from dl_cpc.ocpc_detail_report_hourly_v3
//         |where cost >100000 and version='novel_v1' and `date`='2019-03-14' and `hour`='23'
//         |group by case when cpa_ratio<0.8 then '<0.8'
//         |when cpa_ratio>1.2 then '>1.2'
//         |else 'ok' end
//       """.stripMargin

    // 生成基础数据
    val sql1=
      s"""
         |select
         |  dt as `date`,
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
    spark.sql(sql1).select("unitid","usertype","adclass","media","money_byunit","isclick_byunit",
      "isshow_byunit","`date`")
      .repartition(1).write.mode("overwrite").insertInto("dl_cpc.OcpcLaunchdata")
    //标记直投暗投 choose 1 直投，choose 0 暗投
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
         |  from dl_cpc.OcpcLaunchdata
         |  where `date`='$date'
         |  group by
         |  unitid
         |) a
       """.stripMargin
    println(sql2)
    val data1=spark.sql(sql2)

      spark.sql(s"select * from dl_cpc.OcpcLaunchdata where media = 'novel' and `date`='$date'")
        .join(data1,Seq("unitid"))
        .select("unitid","usertype","adclass","media","money_byunit","isclick_byunit",
        "isshow_byunit","qtt_money","novel_money","choose","`date`")
        .repartition(1).write.mode("overwrite").insertInto("dl_cpc.OcpcLaunchdata2")

    //直投暗投总体分析
    val sql3=
      s"""
         |select
         |  a.day as `date`,
         |  choose,
         |  sum(case WHEN isclick == 1 then price else 0 end) as money,
         |  round(sum(isclick)*100 / sum(isshow),3) as ctr,
         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isclick),3) as acp
         |from
         |(
         |  select *
         |  from dl_cpc.cpc_novel_union_events
         |  where day = '$date'
         |  and isshow = 1
         |  and adsrc = 1
         |  and media_appsid in ("80001098","80001292")
         |  AND userid > 0
         |  AND (charge_type IS NULL OR charge_type = 1)
         |) a
         |left join dl_cpc.OcpcLaunchdata2 b
         |on a.unitid=b.unitid and b.`date` = '$date'
         |group by
         |  a.day,
         |  choose
         |order by
         |  choose
       """.stripMargin

    println(sql3)
        val data3=spark.sql(sql3).filter("choose is not null")
        val money_overall=data3.select("money").rdd.map(x => x.getAs[Long]("money")).reduce(_+_).toDouble
        val data3result=data3.withColumn("sum_money_ratio",round(col("money")/money_overall*100,3))
          .select("choose","money","sum_money_ratio","cpm","acp","ctr","`date`")

    val table1 = "report2.midu_ocpc_launch_overall"
    val deleteSql1 = s"delete from $table1 where `date` = '$date'"
    OperateMySQL.update(deleteSql1)
    OperateMySQL.insert(data3result,table1)
    //直投暗投ocpc及cpc分析
    val sql4=
      s"""
         |select
         |  a.day as `date`,
         |  choose,
         |  case when length(ocpc_log) > 0 then 'ocpc'
         |  else 'cpc' end as mode,
         |  sum(case WHEN isclick == 1 then price else 0 end) as money,
         |  round(sum(isclick)*100 / sum(isshow),3) as ctr,
         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isclick),3) as acp
         |from
         |(
         |   select *
         |  from dl_cpc.cpc_novel_union_events
         |  where day = '$date'
         |  and isshow = 1
         |  and adsrc = 1
         |  and media_appsid in ("80001098","80001292")
         |  AND userid > 0
         |  AND (charge_type IS NULL OR charge_type = 1)
         |) a
         |left join dl_cpc.OcpcLaunchdata2 b
         |on a.unitid=b.unitid and b.`date` = '$date'
         |group by
         |  a.day,
         |  choose,
         |  case when length(ocpc_log) > 0 then 'ocpc'
         |  else 'cpc' end
         |order by
         |  choose
       """.stripMargin

    println(sql4)
    val data4result=spark.sql(sql4).filter("choose is not null")
      .withColumn("sum_money_ratio",round(col("money")/money_overall*100,3))
      .select("choose","mode","money","sum_money_ratio","cpm","acp","ctr","`date`")

    val table2 = "report2.midu_ocpc_launch_ocpc_cpc"
    val deleteSql2 = s"delete from $table2 where `date` = '$date'"
    OperateMySQL.update(deleteSql2)
    OperateMySQL.insert(data4result,table2)

  //用户类型分析：其他非企正企
  val sql5=
    s"""
       |select
       |  a.day as `date`,
       |  choose,
       |  a.usertype,
       |  sum(case WHEN isclick == 1 then price else 0 end) as money,
       |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
       |  if(sum(isclick)>0,round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isclick),3),0) as acp,
       |  round(sum(isclick)*100 / sum(isshow),3) as ctr
       |from
       |(
       |  select *
       |  from dl_cpc.cpc_novel_union_events
       |  where day = '$date'
       |  and isshow = 1
       |  and adsrc = 1
       |  and media_appsid in ("80001098","80001292")
       |  AND userid > 0
       |  AND (charge_type IS NULL OR charge_type = 1)
       |) a
       |left join dl_cpc.OcpcLaunchdata2 b
       |on a.unitid=b.unitid and b.`date` = '$date'
       |group by
       |  a.day,
       |  choose,
       |  a.usertype
       |order by
       |  choose,
       |  usertype
         """.stripMargin
    println(sql5)

    val data5result=spark.sql(sql5).filter("choose is not null")
          .withColumn("sum_money_ratio",round(col("money")/money_overall*100,3))
          .select("choose","usertype","money","sum_money_ratio","cpm","acp","ctr","`date`")

    val table3 = "report2.midu_ocpc_launch_usertype"
    val deleteSql3 = s"delete from $table3 where `date` = '$date'"
    OperateMySQL.update(deleteSql3)
    OperateMySQL.insert(data5result,table3)

    //用户类型：重点行业
    val sql6=
      s"""
         |select
         |  a.day as `date`,
         |  choose,
         |  case when round(a.adclass/1000000) == 100 then 'app'
         |  when round(a.adclass/1000) == 110110 then 'wz'
         |  else 'other' end as adclass,
         |  sum(case WHEN isclick == 1 then price else 0 end) as money,
         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
         |  round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isclick),3) as acp,
         |  round(sum(isclick)*100 / sum(isshow),3) as ctr
         |from
         |(
         |   select *
         |  from dl_cpc.cpc_novel_union_events
         |  where day = '$date'
         |  and isshow = 1
         |  and adsrc = 1
         |  and media_appsid in ("80001098","80001292")
         |  AND userid > 0
         |  AND (charge_type IS NULL OR charge_type = 1)
         |) a
         |left join dl_cpc.OcpcLaunchdata2 b
         |on a.unitid=b.unitid and b.`date` = '$date'
         |group by
         |  a.day,
         |  choose,
         |  case when round(a.adclass/1000000) == 100 then 'app'
         |  when round(a.adclass/1000) == 110110 then 'wz'
         |  else 'other' end
         |order by
         |  choose
         """.stripMargin

    println(sql6)

    val data6result=spark.sql(sql6).filter("choose is not null")
      .withColumn("sum_money_ratio",round(col("money")/money_overall*100,3))
      .select("choose","adclass","money","sum_money_ratio","cpm","acp","ctr","`date`")

    val table4 = "report2.midu_ocpc_launch_adclass"
    val deleteSql4 = s"delete from $table4 where `date` = '$date'"
    OperateMySQL.update(deleteSql4)
    OperateMySQL.insert(data6result,table4)

      //暗投usertype配比分析
        val sql7=
          s"""
             |select
             |  `date`,
             |  usertype,
             |  sum(qtt_money_1) sum_qtt_money_1,
             |  sum(qtt_money_2) sum_qtt_money_2,
             |  sum(novel_money) sum_novel_money,
             |  round(sum(novel_money)/sum(qtt_money_1)*100,3) as avg_ratio_1,
             |  if(sum(qtt_money_2)>0, round(sum(novel_money)/sum(qtt_money_2)*100,3), 0) as avg_ratio_2,
             |  round(sum(if( ratio >= 2,1,0))/sum(1)*100,3) as gt200,
             |  round(sum(if( ratio >= 1 and ratio < 2,1,0))/sum(1)*100,3) as gt100,
             |  round(sum(if( ratio >= 0.5 and ratio < 1,1,0))/sum(1)*100,3) as gt50,
             |  round(sum(if( ratio < 0.5,1,0))/sum(1)*100,3) as lt50,
             |  round(sum(if( ratio is null,1,0))/sum(1)*100,3) as eq0
             |from
             |(
             |  select
             |  A.`date`,
             |  A.unitid,
             |  A.usertype,
             |  A.money_byunit as qtt_money_1,
             |  if(B.qtt_money is null,0,B.qtt_money) as qtt_money_2,
             |  if(novel_money is null,0,novel_money) as novel_money,
             |  round(novel_money/A.money_byunit,3) as ratio
             |  from dl_cpc.OcpcLaunchdata A
             |  left join dl_cpc.OcpcLaunchdata2 B
             |  on A.unitid = B.unitid and B.choose = 0 and B.`date`= '$date'
             |  where A.media = 'qtt' and A.money_byunit > 0 and A.`date`= '$date'
             |) a
             |group by
             |  `date`,
             |  usertype
             |order by
             |  usertype
               """.stripMargin

    println(sql7)
    val data7result=spark.sql(sql7)
      .select("usertype","sum_qtt_money_1","sum_qtt_money_2","sum_novel_money","avg_ratio_1","avg_ratio_2"
        ,"gt200","gt100","gt50","lt50","eq0","`date`")

    val table5 = "report2.midu_ocpc_launch_usertype_ratio"
    val deleteSql5 = s"delete from $table5 where `date` = '$date'"
    OperateMySQL.update(deleteSql5)
    OperateMySQL.insert(data7result,table5)


    val sql8=
      s"""
         |select
         |  `date`,
         |  case when round(a.adclass/1000000) == 100 then 'app'
         |  when round(a.adclass/1000) == 110110 then 'wz'
         |  else 'notag' end as adclass,
         |  sum(qtt_money_1) sum_qtt_money_1,
         |  sum(qtt_money_2) sum_qtt_money_2,
         |  sum(novel_money) sum_novel_money,
         |  round(sum(novel_money)/sum(qtt_money_1)*100,3) as avg_ratio_1,
         |  if(sum(qtt_money_2)>0, round(sum(novel_money)/sum(qtt_money_2)*100,3), 0) as avg_ratio_2,
         |  round(sum(if( ratio >= 2,1,0))/sum(1)*100,3) as gt200,
         |  round(sum(if( ratio >= 1 and ratio < 2,1,0))/sum(1)*100,3) as gt100,
         |  round(sum(if( ratio >= 0.5 and ratio < 1,1,0))/sum(1)*100,3) as gt50,
         |  round(sum(if( ratio < 0.5,1,0))/sum(1)*100,3) as lt50,
         |  round(sum(if( ratio is null,1,0))/sum(1)*100,3) as eq0
         |from
         |(
         |  select
         |  A.`date`,
         |  A.unitid,
         |  A.adclass,
         |  A.money_byunit as qtt_money_1,
         |  if(B.qtt_money is null,0,B.qtt_money) as qtt_money_2,
         |  if(novel_money is null,0,novel_money) as novel_money,
         |  round(novel_money/A.money_byunit,3) as ratio
         |  from dl_cpc.OcpcLaunchdata A
         |  left join dl_cpc.OcpcLaunchdata2 B
         |  on A.unitid = B.unitid and B.choose = 0 and B.`date`= '$date'
         |  where A.media = 'qtt' and A.money_byunit > 0 and A.`date`= '$date'
         |) a
         |group by
         |  `date`,
         |  case when round(a.adclass/1000000) == 100 then 'app'
         |  when round(a.adclass/1000) == 110110 then 'wz'
         |  else 'notag' end
         """.stripMargin
    println(sql8)
    val data8result=spark.sql(sql8)
      .select("adclass","sum_qtt_money_1","sum_qtt_money_2","sum_novel_money","avg_ratio_1","avg_ratio_2"
        ,"gt200","gt100","gt50","lt50","eq0","`date`")

    val table6 = "report2.midu_ocpc_launch_adclass_ratio"
    val deleteSql6 = s"delete from $table6 where `date` = '$date'"
    OperateMySQL.update(deleteSql6)
    OperateMySQL.insert(data8result,table6)


  }

}
