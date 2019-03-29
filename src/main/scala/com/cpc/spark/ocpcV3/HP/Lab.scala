package com.cpc.spark.ocpcV3.HP

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.SaveMode
import java.util.Properties
import java.sql.{Connection, DriverManager}
import com.typesafe.config.ConfigFactory

object Lab {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().appName("Lab").enableHiveSupport().getOrCreate()

    val sql1 =
      s"""
         | select
         |  case when (`date` = '2019-03-18' and hour >= '21') or (`date` = '2019-03-19' and hour < '21') then '2019-03-19'
         |       when (`date` = '2019-03-19' and hour >= '21') or (`date` = '2019-03-20' and hour < '21') then '2019-03-20'
         |       when (`date` = '2019-03-20' and hour >= '21') or (`date` = '2019-03-21' and hour < '21') then '2019-03-21'
         |       when (`date` = '2019-03-21' and hour >= '21') or (`date` = '2019-03-22' and hour < '21') then '2019-03-22'
         |       when (`date` = '2019-03-22' and hour >= '21') or (`date` = '2019-03-23' and hour < '21') then '2019-03-23'
         |       when (`date` = '2019-03-23' and hour >= '21') or (`date` = '2019-03-24' and hour < '21') then '2019-03-24'
         |       when (`date` = '2019-03-24' and hour >= '21') or (`date` = '2019-03-25' and hour < '21') then '2019-03-25'
         |       when (`date` = '2019-03-25' and hour >= '21') or (`date` = '2019-03-26' and hour < '21') then '2019-03-26'
         |       when (`date` = '2019-03-26' and hour >= '21') or (`date` = '2019-03-27' and hour < '21') then '2019-03-27'
         |       when (`date` = '2019-03-27' and hour >= '21') or (`date` = '2019-03-28' and hour < '21') then '2019-03-28'
         |  end as dt,
         |  identifier
         | from dl_cpc.ocpc_pb_result_hourly  --该表中的identifier已经去直投了
         |where ((`date` = '2019-03-18' and hour >= '21') or (`date` = '2019-03-28' and hour < '21'))
         |  and version = 'hottopicv1'
       """.stripMargin

    val date_identifier = spark.sql(sql1).distinct().toDF()
    date_identifier.createOrReplaceTempView("date_identifier")

    val sql2 =
      s"""
         |  select
         |   a.userid,
         |   a.`date`,
         |   a.identifier,
         |   case
         |     when c.identifier is not NULL and exptags like "%hot_topic%" and length(ext_string['ocpc_log']) > 0  then '暗投ocpc'
         |     when c.identifier is not NULL and                                length(ext_string['ocpc_log']) = 0  then '暗投cpc'
         |     else '直投'
         |    end as label, --这里的ocpc是指付费方式为cpc下面的ocpc
         |   sum(isshow)                                                                    as show_n,
         |         sum(isclick)                                                             as click_n,
         |   round(sum(case WHEN isclick = 1 then price else 0 end)/100, 3)                 as money
         |FROM
         |     (  select
         |         ext_string['ocpc_log'] as ocpc_log,
         |         cast(unitid as string) as identifier,
         |         *
         |        from dl_cpc.cpc_hot_topic_union_log
         |       WHERE `date` between '2019-03-19' and '2019-03-28'
         |         and isshow = 1 --已经展示
         |         and media_appsid = '80002819'
         |         and ext['antispam'].int_value = 0  --反作弊标记：1作弊，0未作弊
         |         AND userid > 0 -- 广告主id
         |         and adsrc = 1  -- cpc广告（我们这边的广告，非外部广告）
         |         AND ( ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1 ) --charge_type: 计费类型
         |     ) a
         |left join
         |     (
         |        select
         |         dt,
         |         identifier
         |        from date_identifier
         |      ) c on a.`date` = c.dt and a.identifier = c.identifier
         |GROUP BY
         |   a.userid,
         |   a.`date`,
         |   a.identifier,
         |   case
         |     when c.identifier is not NULL and exptags like "%hot_topic%" and length(ext_string['ocpc_log']) > 0  then '暗投ocpc'
         |     when c.identifier is not NULL and                                length(ext_string['ocpc_log']) = 0  then '暗投cpc'
         |     else '直投'
         |    end
       """.stripMargin

    val result = spark.sql(sql2)
    result.write.mode("overwrite").saveAsTable("test.detail_sjq")

  }

}








