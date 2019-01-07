package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object report_userprofile_effect {
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
     |  a.isclick,
     |  a.isshow,
     |  case when charge_type=2 then a.price*1.0/1000 else a.price end as price,
     |  a.interests
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
     |  COALESCE(a.isclick, 0) as isclick,
     |  a.isshow,
     |  COALESCE(a.price, 0) price,
     |  a.interests,
     |  case when b.label2=1 or c.label3=1 then 1 else 0 end as iscvr
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

    println(sqlRequest2)
    val base = spark.sql(sqlRequest2).repartition(10000).persist(StorageLevel.MEMORY_AND_DISK_SER)
    print("base——count" + base.count())

    // recalculation with groupby of userid and uid
    base.createOrReplaceTempView("tmpTable")

    val result =
      s"""
         |Select
         |  userid,
         |  SUM(price) as cost,
         |  SUM(isclick) as ctr,
         |  SUM(iscvr) as cvr,
         |  SUM(isshow) as show
         |FROM tmpTable GROUP BY userid
       """.stripMargin

    spark.sql(result).createOrReplaceTempView("total")

    val result1 =
      s"""
         |select userid, tag, SUM(price) as costWithTag, SUM(isclick) as ctrWithTag, SUM(iscvr) as cvrWithTag, SUM(isshow) as showWithTag
         |from (Select
         |  searchid,
         |  uid,
         |  userid,
         |  isclick,
         |  isshow,
         |  price,
         |  iscvr,
         |  split(interest, '=')[1] as tag
         |FROM tmpTable lateral view explode(split(interests, ',')) a as interest) ta group by userid, tag
       """.stripMargin

    spark.sql(result1).createOrReplaceTempView("withtag")

    val result2 =
      s"""
         |insert into dl_cpc.cpc_profileTag_report_daily partition (`date`='$date')
         |Select ta.userid, ta.tag, cost-ta.costWithTag, ctr-ctrWithTag, cvt-cvrWithTag, show-showWithTag,
         |costWithTag, ctrWithTag, cvrWithTag, showWithTag
         |from withtag ta left join total tb on ta.userid=tb.userid
       """.stripMargin

    spark.sql(result2)

    unionlog.unpersist()
    base.unpersist()

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

}