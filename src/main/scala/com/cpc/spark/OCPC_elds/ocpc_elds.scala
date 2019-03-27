package com.cpc.spark.OCPC_elds

import java.util.Properties

import com.cpc.spark.novel.OperateMySQL
import com.cpc.spark.tools.{CalcMetrics, OperateMySQL}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * @author Yulin
  * @date 2019/03/27 17:30
  */

object ocpc_elds {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val spark = SparkSession.builder()
      .appName(s"ocpc_elds date = $date ")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val tmpDate = date.replace("-", "")
    val Sql =
      s"""
         |select
         |*
         |from
         |(select
         |a.identifier,
         |a.userid,
         |a.cpa_given,
         |a.cpa_real,
         |a.cpa_ratio,
         |a.click,
         |a.conversion,
         |a.ctr,
         |a.cost/100 as cost,
         |a.acp,
         |a.avg_k,
         |a.pre_cvr,
         |a.post_cvr,
         |b.`date` as after_date,
         |b.identifier as after_identifier,
         |b.userid as after_userid,
         |b.cpa_given as after_cpa_given,
         |b.cpa_real as after_cpa_real,
         |b.cpa_ratio as after_cpa_ratio,
         |b.click as after_click,
         |b.conversion as after_conversion,
         |b.ctr as after_ctr,
         |b.cost/100 as after_cost,
         |b.acp as after_acp,
         |b.avg_k as after_avg_k,
         |b.pre_cvr as after_pre_cvr,
         |b.post_cvr as after_post_cvr,
         |if(a.cost is not null or a.cost != 0,(a.cost-b.cost)/a.cost,0) as cost_ratio,
         |a.`date`,
         |from
         |(select
         |*
         |from dl_cpc.ocpc_detail_report_hourly_v4
         |where `date` = date_sub('$date',1)
         |and hour = '23'
         |and version = 'qtt_demo'
         |and conversion_goal = 3 )a
         |full outer join
         |(select
         |*
         |from dl_cpc.ocpc_detail_report_hourly_v4
         |where `date` = '$date'
         |and hour = '23'
         |and version = 'qtt_demo'
         |and conversion_goal = 3 )b on a.identifier=b.identifier )p
             """.stripMargin
   println(Sql)
    val union = spark.sql(Sql)

    //保存到临时表里
    union.createOrReplaceTempView("union")
    println ("union is successful! ")

    val totalsql =
      s"""
         |select
         | a.unitid_cnt,
         | a.userid_cnt,
         | a.new_unitid_cnt,
         | a.no_unitid_cnt,
         | a.dl_unitid_cnt,
         | a.dl_unitid_cnt/(a.unitid_cnt-a.new_unitid_cnt+no_unitid_cnt) as dl_ratio,
         | a.dl_b_cost-a.dl_a_cost as gap_cost
         |from
         |(select
         |count(distinct case when after_identifier is not null then after_identifier else null end) as unitid_cnt,
         |count(distinct case when after_userid is not null then after_userid else null end) as userid_cnt,
         |count(distinct case when `date` is null then after_identifier else null end) as new_unitid_cnt,
         |count(distinct case when after_date is null then identifier else null end) as no_unitid_cnt,
         |count(distinct case when cost>1000 and (cost_ratio>0.3 or after_cost is null) then identifier else null end) as dl_unitid_cnt,
         |sum(case when cost>1000 and (cost_ratio>0.3 or after_cost is null) then cost else null end) as dl_b_cost,
         |sum(case when cost>1000 and (cost_ratio>0.3 or after_cost is null) then after_cost else null end) as dl_a_cost
         |from
         |union  )a
             """.stripMargin

    println(totalsql)
    val total = spark.sql(totalsql)
    //保存到临时表里
    total.write.mode("overwrite").saveAsTable("test.ocpc_elds_total")
    println("total is successful! ")
   val dlsql =
     s"""
        |select
        |*
        |from
        |union a
        |where a.cost>1000
        |and (cost_ratio>0.3 or after_cost is null)
             """.stripMargin

    println(dlsql)

    val result = spark.sql(dlsql)
    result.show(10)
    result.repartition(1)
      .write
      .mode("overwrite")
      .insertInto("dl_cpc.ocpc_elds_data")
    println("result is successful! ")
  }

}
