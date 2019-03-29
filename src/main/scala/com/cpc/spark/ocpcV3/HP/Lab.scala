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
         |where `date` between '2019-03-18' and '2019-03-28'
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
    val usertype = getUsertype(spark)
    usertype.write.mode("overwrite").saveAsTable("test.usertype_sjq")

  }

  def getUsertype(spark: SparkSession) ={
    val sql =
      s"""
         |select
         | userid,
         | usertype,
         | sum(isshow) as shown
         |from dl_cpc.slim_union_log
         |where dt between '2019-03-19' and '2019-03-28'
         |  and userid in (1576960,	1576675,	1543127,	1541932,	1589298,	1590326,	1585034,	1589171,	1571948,	1580884,	1585017,	1540400,	1525122,	1582123,	1535886,
         |1581268,	1585263,	1587514,	1587235,	1565604,	1547207,	1580074,	1577232,	1581744,	1586487,	1503850,	1588107,	1514425,	1583382,	1565285,
         |1558158,	1587619,	1533758,	1587805,	1586672,	1570905,	1580870,	1570426,	1569155,	1585258,	1586333,	1583566,	1559493,	1570923,	1586378,
         |1584173,	1582244,	1566570,	1544609,	1587342,	1587982,	1587669,	1583276,	1546585,	1563368,	1587860,	1569058,	1587665,	1551011,	1579388,
         |1581037,	1553589,	1574345,	1575449,	1552565,	1581635,	1587354,	1581406,	1582024,	1557671,	1581753,	1566783,	1587864,	1587646,	1588587,
         |1551348,	1573164,	1587791,	1579644,	1586707,	1576517,	1565596,	1534362,	1587859,	1575350,	1538231,	1579766,	1579684,	1542903,	1565696,
         |1555168,	1533696,	1567825,	1555787,	1576693,	1590655,	1587998,	1587338,	1581438,	1563165,	1584816,	1592575,	1522698,	1589627,	1589095,
         |1587033,	1566487,	1587519,	1572613,	1587185,	1588928,	1523258,	1589439,	1566499,	1589825,	1590347,	1573096,	1576692,	1508354,	1574891,
         |1521839,	1590688,	1555580,	1590730,	1589827,	1590911,	1586535,	1572744,	1581157,	1587043,	1587637,	1556856,	1590783,	1586673,	1553638,
         |1592796,	1554537,	1586494,	1583449,	1526997,	1589816,	1591450,	1588104,	1587638,	1557971,	1592535,	1593673,	1589982,	1566936,	1593061,
         |1591421,	1592661,	1546477,	1542405,	1555574,	1588947,	1520926,	1589540,	1587732,	1584142,	1551959,	1588506,	1584147,	1576478,	1587402,
         |1582280,	1589675,	1563406,	1564480,	1586643,	1562064,	1592740,	1517073,	1563501,	1591456,	1549892,	1592393,	1587538,	1589125,	1580059,
         |1589403,	1591985,	1564196,	1587926,	1587133,	1590905,	1592986,	1591516,	1580882,	1572427,	1588519,	1587935 )
         |  and isshow = 1 --已经展示
         |  and antispam = 0  --反作弊标记：1作弊，0未作弊
         |  and adsrc = 1  -- cpc广告（我们这边的广告，非外部广告）
         |  AND ( charge_type IS NULL OR charge_type = 1 )
         |group by userid,
         | usertype
       """.stripMargin
    val df = spark.sql(sql)
    df
  }

}








