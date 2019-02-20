package com.cpc.spark.ocpcV3.HotTopicOcpc.HotTopic_BI_report

import java.util.Properties
import java.sql.{Connection, DriverManager}
import org.apache.spark.sql.{SparkSession, DataFrame}
import com.typesafe.config.ConfigFactory

object BiReport {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val date = args(0)
    val sql1 =
      s"""
         |select
         |  unitid,
         |  usertype,
         |  adclass,
         |  case
         |    when media_appsid = '80002819' then 'hottopic'
         |    else 'qtt'
         |    end as media,
         |  sum(if( isclick = 1, price, 0)) as money,
         |  sum(isshow)  as show_cnt,
         |  sum(isclick) as click_cnt,
         |  dt as `date`
         |from dl_cpc.slim_union_log
         |where dt = '$date'
         |  and adsrc = 1
         |  and userid >0
         |  and isshow = 1
         |  and antispam = 0
         |  and (charge_type is NULL or charge_type = 1)
         |  and media_appsid in ('80000001', '80000002', '80002819')
         |group by
         |  unitid,
         |  usertype,
         |  adclass,
         |  case
         |    when media_appsid = '80002819' then 'hottopic'
         |    else 'qtt'
         |    end,
         |  dt
       """.stripMargin
//    create table dl_cpc.unit_ect_summary_sjq
//    ( unitid    bigint, usertype  bigint, adclass   bigint, media     string, money     bigint, show_cnt  bigint, click_cnt bigint )
//    comment "group by unitid, usertype, adclass, media to sum price|isclick = 1, isshow, isclick"
//    partitioned by (`date` string);
    val tb1 = "dl_cpc.unit_ect_summary_sjq"
    spark.sql(sql1).select( "unitid", "usertype", "adclass", "media", "money", "show_cnt", "click_cnt", "date" ).write.mode("overwrite").insertInto(tb1)
  }

}
