package com.cpc.spark.ml.recallReport

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession

object bscvr_exp_report {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bscvr exp report")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._
    val cal1 = Calendar.getInstance()
    cal1.add(Calendar.DATE, -1)
    val tardate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    cal1.add(Calendar.DATE, -2)
    val startdate = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)

    spark.sql(s"""SELECT
         sum(isshow) as imp,
         sum(isclick) as click,
         sum(if(tc.label > 0, 1, 0)) as iscvr,
         exp,
          10000 as adslot_type,
         'all_industry' as industry,
         round(sum(isclick)*100.0 / sum(isshow),3) as ctr,
         sum(if(isclick=1,price,0)) as cost,
         round(sum(if(isclick=1,price,0))*10.0/sum(isshow),3) as cpm,
         round(sum(if(isclick=1,price,0))*1.0/count(distinct uid),3) as arpu,
         count(distinct uid) as uv,
         round(sum(if(tc.label > 0, 1, 0))*100.0/sum(isclick),3) as cvr,
          'exp_unitid' as unitid
          from
         (select searchid, ideaid, isshow, isclick, uid,industry,
         if(exptags like '%bscvrexp:0.3%', 'enabled0.3',
         if(exptags like '%bscvrexp:0.5%', 'enabled0.5', 'control')) as exp,
         adslot_type, cast(unitid as bigint) as unitid,
         price FROM dl_cpc.slim_union_log
         WHERE dt='$tardate'
         and media_appsid  in ('80000001', '80000002') and isshow > 0
         and adsrc = 1
         AND userid > 0
         and uid not like '%000000%'
         and (charge_type = 1 or charge_type is null)) ta
         join
         (select cast(unitid as bigint) as unitid
         from dl_cpc.cpc_recall_bsExp_unitid_exp where date='$startdate' group by unitid) tb on (ta.unitid=tb.unitid)
         left join
         (select searchid,ideaid, 1 as label from dl_cpc.dl_conversion_by_industry where dt='$tardate'
          and isreport=1 group by searchid,ideaid) tc
         on (ta.searchid = tc.searchid and ta.ideaid=tc.ideaid)
         group by ta.exp
""".stripMargin).repartition(1).createOrReplaceTempView("unitid_total")


    spark.sql(s"""SELECT
|         sum(isshow) as imp,
|         sum(isclick) as click,
|         sum(if(tc.label > 0, 1, 0)) as iscvr,
|         exp,
|         adslot_type,
|         industry,
|         round(sum(isclick)*100.0 / sum(isshow),3) as ctr,
|         sum(if(isclick=1,price,0)) as cost,
|         round(sum(if(isclick=1,price,0))*10.0/sum(isshow),3) as cpm,
|         round(sum(if(isclick=1,price,0))*1.0/count(distinct uid),3) as arpu,
|         count(distinct uid) as uv,
|         round(sum(if(tc.label > 0, 1, 0))*100.0/sum(isclick),3) as cvr,
|         ta.unitid
|          from
|         (select searchid, ideaid, isshow, isclick, uid,industry,
|         if(exptags like '%bscvrexp:0.3%', 'enabled0.3',
|         if(exptags like '%bscvrexp:0.5%', 'enabled0.5', 'control')) as exp,
|         adslot_type, cast(unitid as bigint) as unitid,
|         price FROM dl_cpc.slim_union_log
|         WHERE dt='$tardate' and media_appsid  in ('80000001', '80000002') and isshow > 0
|         and adsrc = 1
|         AND userid > 0
|         and uid not like '%000000%'
|         and (charge_type = 1 or charge_type is null)) ta
|         join
|         (select cast(unitid as bigint) as unitid
|         from dl_cpc.cpc_recall_bsExp_unitid_exp where date='$startdate' group by unitid) tb on (ta.unitid=tb.unitid)
|         left join
|         (select searchid,ideaid, 1 as label from dl_cpc.dl_conversion_by_industry where dt='$tardate'
|          and isreport=1 group by searchid,ideaid) tc
|         on (ta.searchid = tc.searchid and ta.ideaid=tc.ideaid)
|         group by ta.exp,ta.adslot_type,ta.unitid,industry order by adslot_type,ta.unitid,industry,cpm
""".stripMargin).repartition(10).createOrReplaceTempView("unitid_detail")



    spark.sql(s"""SELECT
|         sum(isshow) as imp,
|         sum(isclick) as click,
|         sum(if(tc.label > 0, 1, 0)) as iscvr,
|         exp,
|         10000 as adslot_type,
|         'all_industry' as industry,
|         round(sum(isclick)*100.0 / sum(isshow),3) as ctr,
|         sum(if(isclick=1,price,0)) as cost,
|         round(sum(if(isclick=1,price,0))*10.0/sum(isshow),3) as cpm,
|         round(sum(if(isclick=1,price,0))*1.0/count(distinct uid),3) as arpu,
|         count(distinct uid) as uv,
|         round(sum(if(tc.label > 0, 1, 0))*100.0/sum(isclick),3) as cvr,
|         'all' as unitid
|          from
|         (select searchid, ideaid, isshow, isclick, uid,industry,
|         if(exptags like '%bscvrexp:0.3%', 'enabled0.3',
|         if(exptags like '%bscvrexp:0.5%', 'enabled0.5', 'control')) as exp,
|         adslot_type, cast(unitid as bigint) as unitid,
|         price FROM dl_cpc.slim_union_log
|         WHERE dt='$tardate'
|         and media_appsid  in ('80000001', '80000002') and isshow > 0
|         and adsrc = 1
|         AND userid > 0
|         and uid not like '%000000%'
|         and (charge_type = 1 or charge_type is null)) ta
|         left join
|         (select searchid,ideaid, 1 as label from dl_cpc.dl_conversion_by_industry where dt='$tardate'
|         and isreport=1 group by searchid,ideaid) tc
|         on (ta.searchid = tc.searchid and ta.ideaid=tc.ideaid)
|         group by ta.exp order by cpm
""".stripMargin).repartition(1).createOrReplaceTempView("unitid_all")

    spark.sql(
      s"""
         |select * from unitid_total
         |union
         |select * from unitid_detail
         |union
         |select * from unitid_all
      """.stripMargin).repartition(1).createOrReplaceTempView("bsExp_report")

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_recall_bsExp_report partition (date='$tardate')
         |select * from bsExp_report
      """.stripMargin)
  }
}
