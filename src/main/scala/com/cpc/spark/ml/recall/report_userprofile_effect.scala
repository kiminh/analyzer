package com.cpc.spark.ml.recall

import java.sql.DriverManager
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.cpc.spark.small.tool.InsertReportAdslotVideoDownload.{clearReportData, mariaReport2dbProp, mariaReport2dbUrl}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object report_userprofile_effect {
  var mariaReport2dbUrl = ""
  val mariaReport2dbProp = new Properties()
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    cal.add(Calendar.DATE, -179)
    val date1 = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
/**
    val sqlRequest1 =
      s"""
         | select
         |  a.searchid,
         |  a.uid,
         |  a.ideaid,
         |  b.userid,
         |  a.isclick,
         |  a.isshow,
         |  case when charge_type=2 then a.price*1.0/1000 else a.price end as price,
         |  a.interests,
         |  b.tag
         | from
         |      (
         |        select userid,ideaid, isshow, isclick, searchid, uid, interests, case when isclick=1 or ext['charge_type'].int_value=2 then price else 0 end as price, ext['charge_type'].int_value as charge_type, ext['adclass'].int_value as adclass
         |        from dl_cpc.cpc_union_log
         |        where date='$date'
         |        and media_appsid  in ("80000001", "80000002", "80000006", "800000062", "80000064", "80000066","80000141")
         |        and isshow = 1
         |        and ext['antispam'].int_value = 0
         |        and ideaid > 0
         |        and adsrc = 1
         |        and userid is not null
         |        and adslot_type!=7
         |        union
         |        select info.userid,info.ideaid, info.isshow, info.isclick, searchid, uid, interests, case when info.isclick=1 or ext['charge_type'].int_value=2 then info.price else 0 end as price, ext['charge_type'].int_value as charge_type, ext['adclass'].int_value as adclass
         |        from dl_cpc.cpc_union_log
         |        lateral view explode(motivation) b AS info
         |        where date='$date'
         |        and media_appsid  in ("80000001", "80000002", "80000006", "800000062", "80000064", "80000066","80000141")
         |        and ext['antispam'].int_value = 0
         |        and info.isshow=1
         |        and adslot_type=7
         |      ) a
         |join
         |      (select tag, userid from dl_cpc.cpc_tag_userid_all where thedate>'$date1' group by tag, userid) b
         |on a.userid=b.userid or a.adclass=b.userid
      """.stripMargin
  */

val sqlRequest1 =
  s"""
     | select
     |  a.searchid,
     |  a.uid,
     |  a.ideaid,
     |  a.userid,
     |  adslot_type,
     |  a.unitid,
     |  a.isclick,
     |  a.isshow,
     |  case when charge_type=2 then a.price*1.0/1000 else a.price end as price,
     |  a.interests
     | from
     |      (
     |        select userid,adslot_type,ideaid,unitid, isshow, isclick, searchid, uid, interests, case when isclick=1 or charge_type=2 then price else 0 end as price, charge_type, adclass
     |        from dl_cpc.cpc_basedata_union_events
     |        where day='$date'
     |        and media_appsid  in ("80000001", "80000002", "80000006", "800000062", "80000064", "80000066","80000141")
     |        and isshow = 1
     |        and ideaid > 0
     |        and adsrc = 1
     |        and userid is not null
     |      ) a
      """.stripMargin
    //charge_type 1 cpc, 2 cpm, 3 cpa, 0 free
    val unionlog = spark.sql(sqlRequest1).repartition(10000).persist(StorageLevel.DISK_ONLY)
    println(unionlog.count())
    unionlog.createOrReplaceTempView("unionlog_table")
/**
    val sqlRequest2 =
      s"""
         |select
         |  a.searchid,
         |  a.uid,
         |  a.userid,
         |  COALESCE(a.isclick, 0) as isclick,
         |  a.isshow,
         |  COALESCE(a.price, 0) price,
         |  a.tag,
         |  a.interests,
         |  COALESCE(b.label2, 0) as iscvr1,
         |  COALESCE(c.label3, 0) as iscvr2
         |from
         |  unionlog_table as a
         |left join
         |  (select searchid,ideaid, max(label2) as label2 from dl_cpc.ml_cvr_feature_v1 where date='$date' group by searchid,ideaid) as b
         |on
         |  a.searchid=b.searchid and a.ideaid=b.ideaid
         |left join
         |  (select searchid,ideaid, max(label) as label3 from dl_cpc.ml_cvr_feature_v2 where date='$date' group by searchid,ideaid) as c
         |on
         |  a.searchid=c.searchid and a.ideaid=c.ideaid
       """.stripMargin
  */
val sqlRequest2 =
  s"""
     |select
     |  a.searchid,
     |  a.uid,
     |  a.userid,
     |  a.adslot_type,
     |  a.ideaid,
     |  a.unitid,
     |  COALESCE(a.isclick, 0) as isclick,
     |  a.isshow,
     |  COALESCE(a.price, 0) price,
     |  a.interests,
     |  if(c.searchid is null,0,1) iscvr
     |from
     |  unionlog_table as a
     | join
     | (
     |  select * from
     |  dl_cpc.dw_unitid_detail
     |  where day='$date'
     |  and conversion_target[0] is not null
     |  and conversion_target[0] not in ('none','site_uncertain')
     | ) b
     | on a.unitid=b.unitid
     |left join
     | (select distinct aa.searchid, aa.ideaid from dl_cpc.cpc_conversion aa join dl_cpc.dw_unitid_detail bb
     |   on aa.unitid=bb.unitid and bb.day = '$date' and bb.conversion_target[0] not in ('none','site_uncertain')
     |   and array_contains(aa.conversion_target,bb.conversion_target[0])
     |   where aa.day = '$date') as c
     |on
     |  a.searchid=c.searchid and a.ideaid=c.ideaid
    """.stripMargin

    val base = spark.sql(sqlRequest2).repartition(10000).persist(StorageLevel.MEMORY_AND_DISK_SER)
    print("base——count" + base.count())

    // recalculation with groupby of userid and uid
    base.createOrReplaceTempView("tmpTable")

    val result =
      s"""
         |Select
         |  userid,
         |  unitid,
         |  ideaid,
         |  adslot_type,
         |  SUM(price) as cost,
         |  SUM(isclick) as ctr,
         |  SUM(iscvr) as cvr,
         |  SUM(isshow) as show
         |FROM tmpTable GROUP BY userid,unitid,ideaid,adslot_type
       """.stripMargin


    spark.sql(result).repartition(5000).createOrReplaceTempView("total")

    val result1 =
      s"""
         |select userid, tag,unitid,ideaid,adslot_type,
         |SUM(price) as costWithTag, SUM(isclick) as ctrWithTag, SUM(iscvr) as cvrWithTag,
         |SUM(isshow) as showWithTag
         |from (Select
         |  searchid,
         |  uid,
         |  userid,
         |  unitid,
         |  ideaid,
         |  adslot_type,
         |  isclick,
         |  isshow,
         |  price,
         |  iscvr,
         |  split(interest, '=')[0] as tag
         |FROM tmpTable lateral view explode(split(interests, ',')) a as interest
         |where interest like '%=100') ta group by userid, tag,unitid,ideaid,adslot_type
       """.stripMargin

    print(result1)
    spark.sql(result1).repartition(500).createOrReplaceTempView("withtag")
    val result2 =
      s"""
         |insert overwrite table dl_cpc.cpc_profileTag_report_daily_v2 partition (`date`='$date')
         |Select ta.userid,ta.unitid, ta.ideaid, ta.adslot_type, ta.tag,
         | ta.showWithTag, show-showWithTag,
         | ta.ctrWithTag, ctr-ctrWithTag,
         | ta.costWithTag, cost-ta.costWithTag,
         | ta.cvrWithTag,
         | cvr-ta.cvrWithTag
         |from withtag ta left join total tb on ta.userid=tb.userid and ta.unitid=tb.unitid and ta.ideaid=tb.ideaid and ta.adslot_type=tb.adslot_type
       """.stripMargin

    print(result2)
    spark.sql(result2)


    //    连接adv_test
    val jdbcProp = new Properties()
    val jdbcUrl = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    jdbcProp.put("user", "adv_live_read")
    jdbcProp.put("password", "seJzIPUc7xU")
    jdbcProp.put("driver", "com.mysql.jdbc.Driver")

    //从adv后台mysql获取人群包的url
    val table="(select value as tag, name from adv.audience_dict where status = 0 group by value,name) as tmp"
    spark.read.jdbc(jdbcUrl, table, jdbcProp).createTempView("tag_table")

    val conf = ConfigFactory.load()
    mariaReport2dbUrl = conf.getString("mariadb.report2_write.url")
    mariaReport2dbProp.put("user", conf.getString("mariadb.report2_write.user"))
    mariaReport2dbProp.put("password", conf.getString("mariadb.report2_write.password"))
    mariaReport2dbProp.put("driver", conf.getString("mariadb.report2_write.driver"))

    clearReportData(date)
    spark.sql(
      s"""
        |select
        |cast(coalesce(ta.userid,0) as int) as userid,
        |cast(coalesce(ta.tag,0) as int) as tag,
        |coalesce(tb.name, 'Unknown') as name,
        |cast(coalesce(ctrwithtag,0) as int) as ctrwithtag,
        |cast(coalesce(ctrwithouttag,0) as int) as ctrwithouttag,
        |coalesce(costwithtag,0) as costwithtag, coalesce(costwithouttag,0) as costwithouttag,
        |cast(coalesce(cvrwithtag,0) as int) as cvrwithtag,
        |cast(coalesce(cvrwithouttag,0) as int) as cvrwithouttag,
        |to_date(date) as date from
        |(select date,userid,tag,sum(ctrwithtag) ctrwithtag,sum(ctrwithouttag) ctrwithouttag,sum(costwithtag) costwithtag,
        |sum(costwithouttag) costwithouttag, sum(cvrwithtag) cvrwithtag,
        |sum(cvrwithouttag) cvrwithouttag from dl_cpc.cpc_profileTag_report_daily_v2
        |where date='$date' group by date,userid, tag) ta left join tag_table tb on ta.tag=tb.tag left join dl_cpc.cpc_userid_tag tc
        |on ta.tag=tc.profile_tag and ta.userid = tc.userid where tc.profile_tag is not null or tb.tag is not null or cast(coalesce(ta.tag,0) as int)<1000
      """.stripMargin).repartition(5).write.mode(SaveMode.Append).jdbc(mariaReport2dbUrl, "report2.cpc_profiletag_report", mariaReport2dbProp)

    unionlog.unpersist()
    base.unpersist()
    /**
    val result2 =
      s"""
         |insert into dl_cpc.cpc_profileTag_report_daily partition (`date`='$date')
         |Select ta.userid, ta.tag, cost-ta.costWithTag, ctr-ctrWithTag, cvr-cvrWithTag, show-showWithTag,
         |costWithTag, ctrWithTag, cvrWithTag, showWithTag
         |from withtag ta left join total tb on ta.userid=tb.userid
       """.stripMargin

    spark.sql(result2)


  */
    /**
    val result =
      s"""
         |insert into dl_cpc.cpc_profileTag_report_daily partition (`date`='$date')
         |Select
         |  userid,
         |  tag,
         |  SUM(CASE WHEN interests not like concat(tag, "=100%") and interests not like concat("%,", tag, "=100%") then price else 0 end) as costWithoutTag,
         |  SUM(CASE WHEN interests not like concat(tag, "=100%") and interests not like concat("%,", tag, "=100%") then isclick else 0 end) as ctrWithoutTag,
         |  SUM(CASE WHEN (iscvr1 = 1 or iscvr2 = 1) and (interests not like concat(tag, "=100%") and interests not like concat("%,", tag, "=100%")) then 1 else 0 end) as cvrWithoutTag,
         |  SUM(CASE WHEN interests not like concat(tag, "=100%") and interests not like concat("%,", tag, "=100%") then isshow else 0 end) as showWithoutTag,
         |  SUM(CASE WHEN interests like concat(tag, "=100%") or interests like concat("%,", tag, "=100%") then price else 0 end) as costWithTag,
         |  SUM(CASE WHEN interests like concat(tag, "=100%") or interests like concat("%,", tag, "=100%") then isclick else 0 end) as ctrWithTag,
         |  SUM(CASE WHEN (iscvr1 = 1 or iscvr2 = 1) and (interests like concat(tag, "=100%") or interests like concat("%,", tag, "=100%"))  then 1 else 0 end) as cvrWithTag,
         |  SUM(CASE WHEN interests like concat(tag, "=100%") or interests like concat("%,", tag, "=100%") then isshow else 0 end) as showWithTag
         |FROM tmpTable GROUP BY userid,tag
       """.stripMargin
      */

  }

  def clearReportData(date: String): Unit = {
    try {
      val conn = DriverManager.getConnection(
        mariaReport2dbUrl,
        mariaReport2dbProp.getProperty("user"),
        mariaReport2dbProp.getProperty("password"))
      val stmt = conn.createStatement()
      val sql =
        """
          |delete from report2.cpc_profiletag_report where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }

}