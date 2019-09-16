package com.cpc.spark.oCPX.oCPC.report

import com.cpc.spark.oCPX.OcpcTools._
import com.cpc.spark.tools.testOperateMySQL
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcExperimentReport{
  def main(args: Array[String]): Unit = {
    /*
    新版报表程序
    1. 从ocpc_unionlog拉取ocpc广告记录
    2. 采用数据关联方式获取转化数据
    3. 统计分ideaid级别相关数据
    4. 统计分conversion_goal级别相关数据
    5. 存储到hdfs
    6. 存储到mysql
     */
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date1 = args(0).toString
    val date2 = args(1).toString

    val sqlRequest =
      s"""
         |select
         |    expid,
         |    tag,
         |	ocpc_flag,
         |    fill,
         |    show,
         |    click,
         |    round(cost/100,2) as charge,
         |    round(abid/100,2) as bid,
         |    round(if(abid>0,cost/abid,0),4) as macro_br,
         |    if(show>0,click/show,0) as ctr,
         |    round(if(click>0,cost/click/100,0),4) as acp,
         |    round(if(click>0,abid/click/100,0),4) as acb,
         |    round(if(show>0,cost*10/show,0),4) as ecpm,
         |    uv,
         |    round(if(uv>0,cost/uv/100,0),4) as arpu
         |from (
         |    select
         |        expid,
         |        tag,
         |		ocpc_flag,
         |        count(distinct tkid) as uv,
         |        sum(isfill) as fill,
         |        sum(isshow) as show,
         |        sum(isclick) as click,
         |        sum(chg) as cost,
         |        sum(cbid) as abid
         |from (
         | select
         |    tkid,
         |    isfill,
         |    isshow,
         |    isclick,
         |	case
         |		when ocpc_step = 2 then 'ocpc'
         |		else 'cpc'
         |	end as ocpc_flag,
         |    case
         |        when array_contains(split(ext_string['exp_ids'], ','), '35457') then 'sy'
         |        when array_contains(split(ext_string['exp_ids'], ','), '35456') then 'dz'
         |        else 'other'
         |    end as expid,
         |    if(adsrc in (1,28),'cpc','dsp') as tag,
         |    if(charge_type=2,isshow*price/1000,isclick*price) as chg,
         |    if(charge_type=2,isshow*bid/1000,if(ocpc_log like '%IsHiddenOcpc:0%', isclick*bid_discounted_by_ad_slot,isclick*bid)) as cbid
         | from dl_cpc.cpc_basedata_union_events
         | where day between '$date1' and '$date2'
         | and adsrc>0
         | and media_appsid in ('80000001', '80000002')
         |) a where expid in ('sy', 'dz')
         |group by expid, tag, ocpc_flag
         |) c order by expid, tag, ocpc_flag desc
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
      .write.mode("overwrite").saveAsTable("test.check_ocpc_smooth_exp20190916a")
  }

}