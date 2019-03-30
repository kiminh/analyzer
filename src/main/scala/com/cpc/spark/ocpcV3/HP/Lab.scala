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
         |  and userid in (1581037,    1581406,    1581836,    1588390,    1576960,    1583276,    1588283,    1589298,    1588107,    1576675,    1588403,    1543127,    1552449,    1585017,    1589171,    1541932,    1570426,    1583384,    1590326,    1590035,
         |1589637,    1566487,    1567729,    1535264,    1554537,    1590076,    1591531,    1590084,    1569058,    1587059,    1590254,    1587366,    1585591,    1533758,    1563600,    1587946,    1584567,    1585232,    1590776,    1589474,
         |1547207,    1567482,    1579766,    1519484,    1590104,    1564079,    1588890,    1539251,    1571994,    1569167,    1539923,    1587914,    1565696,    1589520,    1563171,    1579543,    1589530,    1564584,    1588901,    1588641,
         |1573052,    1590158,    1588598,    1566925,    1591022,    1566761,    1563596,    1571664,    1574862,    1588897,    1572222,    1574148,    1589825,    1588710,    1567871,    1584144,    1567453,    1588590,    1583860,    1583947,
         |1587614,    1565596,    1550052,    1581581,    1543199,    1537508,    1576831,    1520202,    1565843,    1587888,    1518228,    1590212,    1583193,    1592227,    1527654,    1590783,    1587861,    1588109,    1592173,    1588773,
         |1519843,    1591654,    1514929,    1588684,    1583822,    1571996,    1589738,    1521579,    1589739,    1565306,    1591985,    1584841,    1590709,    1587632,    1590655,    1590727,    1592575,    1592856,    1592796,    1584052,
         |1589982,    1586491,    1556107,    1586707,    1570905,    1591503,    1589905,    1575449,    1591356,    1589439,    1589095,    1592654,    1592747,    1574096,    1579684,    1586905,    1542903,    1588937,    1588472,    1579388,
         |1589533,    1580490,    1571705,    1557971,    1566783,    1584173,    1587514,    1588551,    1579644,    1534362,    1590730,    1589570,    1587519,    1540520,    1592877,    1508354,    1588947,    1588928,    1589056,    1581027,
         |1533606,    1549068,    1592435,    1581345,    1523417,    1589270,    1592821,    1587980,    1588363,    1572665,    1592114,    1592790,    1555582,    1592545,    1588688,    1544535,    1591860,    1583546,    1592531,    1592655,
         |1587177,    1533463,    1549785,    1589119,    1573904,    1572956,    1555580,    1526997,    1532869,    1542405,    1523536,    1589217,    1566601,    1589108,    1578363,    1572744,    1586487,    1565280,    1592152,    1525122,
         |1521839,    1582164,    1560379,    1533524,    1585056,    1581644,    1533329,    1585565,    1587619,    1556856,    1559638,    1574891,    1553638,    1586494,    1581157,    1581331,    1564438,    1591498,    1592797,    1546477,
         |1586181,    1587402,    1593632,    1565311,    1591511,    1566085,    1579727,    1586483,    1590529,    1593155,    1593705,    1587353,    1591070,    1592774,    1588085,    1539305,    1593367,    1535812,    1591527,    1588506,
         |1593673,    1592751,    1564929,    1566936,    1518336,    1592535,    1593061,    1584142,    1593752,    1587926,    1570866,    1593924,    1592661,    1591421,    1591888,    1592714,    1594167,    1587662,    1577298,    1578300,
         |1591516,    1590009,    1592393,    1590023,    1589987,    1590425,    1584147,    1591917,    1576478,    1587935,    1589675,    1591721,    1594151,    1587583,    1566499,    1586026,    1588993,    1589067,    1567080,    1586029,
         |1528033,    1587574,    1564480,    1576517,    1586060,    1580871,    1589979,    1589951,    1532173,    1589403,    1580059,    1587319,    1587309,    1587235,    1593477,    1572326,    1589646,    1574464,    1582759,    1581362,
         |1527038,    1568798,    1574147,    1595371,    1591529,    1594038,    1589647,    1589801,    1589891,    1589993,    1518117,    1569538,    1593110,    1560733,    1589362,    1517329,    1594017 )
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








