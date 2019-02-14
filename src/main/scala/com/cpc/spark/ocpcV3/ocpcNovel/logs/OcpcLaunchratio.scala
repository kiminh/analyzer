package com.cpc.spark.ocpcV3.ocpcNovel.logs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.cpc.spark.tools.OperateMySQL

object OcpcLaunchratio {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 生成基础数据
    val date = args(0)
    val sql1=
      s"""
         |select
         |  unitid,
         |  usertype,
         |  adclass,
         |  case
         |  when media_appsid in ('80000001','80000002') then "qtt"
         |  when media_appsid in ('80001098','80001292') then "novel"
         |  ELSE "other" END as media,
         |  sum(case WHEN isclick == 1 then price else 0 end) as money_byunit,
         |  sum(isclick) as isclick_byunit,
         |  sum(isshow) as isshow_byunit,
         |  dt as `date`
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
//    spark.sql(sql1).repartition(1).write.mode("overwrite").insertInto("dl_cpc.OcpcLaunchdata")
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

//      spark.sql(s"select * from dl_cpc.OcpcLaunchdata where media = 'novel' and `date`='$date'")
//        .join(data1,Seq("unitid"))
//        .select("unitid","usertype","adclass","media","money_byunit","isclick_byunit",
//        "isshow_byunit","qtt_money","novel_money","choose","`date`")
//        .write.mode("overwrite").insertInto("dl_cpc.OcpcLaunchdata2")

    //直投暗投总体分析
    val sql3=
      s"""
         |select
         |  a.`date` as `date`,
         |  choose,
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
         |left join dl_cpc.OcpcLaunchdata2 b
         |on a.unitid=b.unitid and b.`date` = '$date'
         |group by
         |  a.`date`,
         |  choose
         |order by
         |  choose
       """.stripMargin

    println(sql3)
        val data3=spark.sql(sql3)
        val money_overall=data3.select("money").rdd.map(x => x.getAs[Long]("money")).reduce(_+_).toDouble
        val data3result=data3.withColumn("sum_money_ratio",round(col("money")/money_overall,3))
          .select("choose","money","sum_money_ratio","cpm","acp","ctr","`date`")
//        data3result.write.mode("overwrite").insertInto("dl_cpc.midu_ocpc_launch_overall")
//
//    val table1 = "report2.midu_ocpc_launch_overall"
//    val deleteSql1 = s"delete from $table1 where `date` = '$date'"
//    OperateMySQL.update(deleteSql1)
//    OperateMySQL.insert(data3result,table1)
    //直投暗投ocpc及cpc分析
    val sql4=
      s"""
         |select
         |  a.`date` as `date`,
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
         |left join dl_cpc.OcpcLaunchdata2 b
         |on a.unitid=b.unitid and b.`date` = '$date'
         |group by
         |  a.`date`,
         |  choose,
         |  case when length(ext_string["ocpc_log"]) > 0 then 'ocpc'
         |  else 'cpc' end
         |order by
         |  choose
       """.stripMargin

    println(sql4)
//    spark.sql(sql4).withColumn("sum_money_ratio",round(col("money")/money_overall,3))
//      .select("choose","mode","money","sum_money_ratio","cpm","acp","ctr","`date`")
//      .write.mode("overwrite").insertInto("dl_cpc.midu_ocpc_launch_ocpc_cpc")
//
//    val table2 = "report2.midu_ocpc_launch_ocpc_cpc"
//    val deleteSql2 = s"delete from $table2 where `date` = '$date'"
//    OperateMySQL.update(deleteSql2)
//    OperateMySQL.insert(data3result,table2)

  //用户类型分析：其他非企正企
  val sql5=
    s"""
       |select
       |  a.`date` as `date`,
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
       |left join dl_cpc.OcpcLaunchdata2 b
       |on a.unitid=b.unitid and b.`date` = '$date'
       |group by
       |  a.`date`,
       |  choose,
       |  ext['usertype'].int_value
       |order by
       |  choose,
       |  ext['usertype'].int_value
         """.stripMargin
    println(sql5)

//        spark.sql(sql5).withColumn("sum_money_ratio",round(col("money")/money_overall,3))
//          .select("choose","usertype","money","sum_money_ratio","cpm","acp","ctr","`date`")
//          .write.mode("overwrite").insertInto("dl_cpc.midu_ocpc_launch_usertype")
//
//    val table3 = "report2.midu_ocpc_launch_usertype"
//    val deleteSql3 = s"delete from $table3 where `date` = '$date'"
//    OperateMySQL.update(deleteSql3)
//    OperateMySQL.insert(data3result,table3)

    //用户类型：重点行业
    val sql6=
      s"""
         |select
         |  a.`date` as `date`,
         |  choose,
         |  case when round(ext['adclass'].int_value/1000000) == 100 then 'app'
         |  when round(ext['adclass'].int_value/1000) == 110110 then 'wz'
         |  else 'other' end as adclass,
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
         |left join dl_cpc.OcpcLaunchdata2 b
         |on a.unitid=b.unitid and b.`date` = '$date'
         |group by
         |  a.`date`,
         |  choose,
         |  case when round(ext['adclass'].int_value/1000000) == 100 then 'app'
         |  when round(ext['adclass'].int_value/1000) == 110110 then 'wz'
         |  else 'other' end
         |order by
         |  choose
         """.stripMargin

    println(sql6)

//    spark.sql(sql6).withColumn("sum_money_ratio",round(col("money")/money_overall,3))
//      .select("choose","adclass","money","sum_money_ratio","cpm","acp","ctr","`date`")
//      .write.mode("overwrite").insertInto("dl_cpc.midu_ocpc_launch_adclass")
//
//    val table4 = "report2.midu_ocpc_launch_adclass"
//    val deleteSql4 = s"delete from $table4 where `date` = '$date'"
//    OperateMySQL.update(deleteSql4)
//    OperateMySQL.insert(data3result,table4)

      //暗投usertype配比分析
        val sql7=
          s"""
             |select
             |  `date`,
             |  usertype,
             |  sum(qtt_money_1) sum_qtt_money_1,
             |  sum(qtt_money_2) sum_qtt_money_2,
             |  sum(novel_money) sum_novel_money,
             |  round(sum(novel_money)/sum(qtt_money_1),3) as avg_ratio_1,
             |  round(sum(novel_money)/sum(qtt_money_2),3) as avg_ratio_2,
             |  round(sum(if( ratio >= 2,1,0))/sum(1),3) as gt200,
             |  round(sum(if( ratio >= 1 and ratio < 2,1,0))/sum(1),3) as gt100,
             |  round(sum(if( ratio >= 0.5 and ratio < 1,1,0))/sum(1),3) as gt50,
             |  round(sum(if( ratio < 0.5,1,0))/sum(1),3) as lt50,
             |  round(sum(if( ratio is null,1,0))/sum(1),3) as eq0
             |from
             |(
             |  select
             |  A.`date`,
             |  A.unitid,
             |  A.usertype,
             |  A.money_byunit as qtt_money_1,
             |  B.qtt_money as qtt_money_2,
             |  novel_money,
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
//    spark.sql(sql7)
//      .select("usertype","sum_qtt_money_1","sum_qtt_money_2","sum_novel_money","avg_ratio_1","avg_ratio_2"
//        ,"gt200","gt100","gt50","lt50","eq0","`date`")
//      .write.mode("overwrite").insertInto("dl_cpc.midu_ocpc_launch_usertype_ratio")
//
//    val table5 = "report2.midu_ocpc_launch_usertype_ratio"
//    val deleteSql5 = s"delete from $table5 where `date` = '$date'"
//    OperateMySQL.update(deleteSql5)
//    OperateMySQL.insert(data3result,table5)


    val sql8=
      s"""
         |select
         |  `date`,
         |  case when round(adclass/1000000) == 100 then 'app'
         |  when round(adclass/1000) == 110110 then 'wz'
         |  else 'notag' end as adclass,
         |  sum(qtt_money_1) sum_qtt_money_1,
         |  sum(qtt_money_2) sum_qtt_money_2,
         |  sum(novel_money),
         |  round(sum(novel_money)/sum(qtt_money),3) as avg_ratio,
         |  round(sum(if( ratio >= 2,1,0))/sum(1),3) as gt200,
         |  round(sum(if( ratio >= 1 and ratio < 2,1,0))/sum(1),3) as gt100,
         |  round(sum(if( ratio >= 0.5 and ratio < 1,1,0))/sum(1),3) as gt50,
         |  round(sum(if( ratio > 0 and ratio < 0.5,1,0))/sum(1),3) as lt50,
         |  round(sum(if( ratio is null,1,0))/sum(1),3) as eq0
         |from
         |(
         |  select
         |  A.`date`,
         |  A.unitid,
         |  A.adclass,
         |  A.money_byunit as qtt_money_1,
         |  B.qtt_money as qtt_money_2,
         |  novel_money,
         |  round(novel_money/A.money_byunit,3) as ratio
         |  from dl_cpc.OcpcLaunchdata A
         |  left join dl_cpc.OcpcLaunchdata2 B
         |  on A.unitid = B.unitid and B.choose = 0 and B.`date`= '$date'
         |  where A.media = 'qtt' and A.money_byunit > 0 and A.`date`= '$date'
         |) a
         |group by
         |  `date`,
         |  case when round(adclass/1000000) == 100 then 'app'
         |  when round(adclass/1000) == 110110 then 'wz'
         |  else 'notag' end
         """.stripMargin
    println(sql8)
    spark.sql(sql8)
      .select("adclass","sum_qtt_money_1","sum_qtt_money_2","sum_novel_money","avg_ratio_1","avg_ratio_2"
        ,"gt200","gt100","gt50","lt50","eq0","`date`")
      .write.mode("overwrite").insertInto("dl_cpc.midu_ocpc_launch_adclass_ratio")

    val table6 = "report2.midu_ocpc_launch_usertype_ratio"
    val deleteSql6 = s"delete from $table6 where `date` = '$date'"
    OperateMySQL.update(deleteSql6)
    OperateMySQL.insert(data3result,table6)


  }

}
