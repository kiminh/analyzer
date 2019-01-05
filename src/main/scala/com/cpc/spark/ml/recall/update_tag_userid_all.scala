package com.cpc.spark.ml.recall

import java.util.{Calendar, Properties}
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

object update_tag_userid_all {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val sqlRequest1 =
      s"""
         | insert into dl_cpc.cpc_tag_all
         | select ta.profile_tag,'$date' from dl_cpc.cpc_userid_tag ta left join dl_cpc.cpc_tag_all tb
         |  on ta.profile_tag=tb.tag where tb.tag is null group by ta.profile_tag
      """.stripMargin
    println(sqlRequest1)
    spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         | insert into dl_cpc.cpc_userid_all
         | select ta.userid,'$date' from dl_cpc.cpc_userid_tag ta left join dl_cpc.cpc_userid_all tb
         |  on ta.userid=tb.userid where tb.userid is null group by ta.userid
      """.stripMargin
    println(sqlRequest2)
    spark.sql(sqlRequest2)

    //    连接adv
    val jdbcProp = new Properties()
    val jdbcUrl = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    jdbcProp.put("user", "adv_live_read")
    jdbcProp.put("password", "seJzIPUc7xU")
    jdbcProp.put("driver", "com.mysql.jdbc.Driver")

    //从adv后台mysql获取人群包的url
    val table=s"(select look_like_id as tag,user_id as userid from adv.look_like where status = 0 and modified_time>'$date') as tmp"
    spark.read.jdbc(jdbcUrl, table, jdbcProp).createTempView("ftp_tag_userid")

    val sqlRequest3 =
      s"""
         | insert into dl_cpc.cpc_tag_all
         | select ta.tag,'$date' from ftp_tag_userid ta left join dl_cpc.cpc_tag_all tb
         |  on ta.tag=tb.tag where tb.tag is null group by ta.tag
      """.stripMargin
    println(sqlRequest3)
    spark.sql(sqlRequest3)

    val sqlRequest4 =
      s"""
         | insert into dl_cpc.cpc_userid_all
         | select ta.userid,'$date' from ftp_tag_userid ta left join dl_cpc.cpc_userid_all tb
         |  on ta.userid=tb.userid where tb.userid is null group by ta.userid
      """.stripMargin
    println(sqlRequest4)
    spark.sql(sqlRequest4)


    //从adv后台mysql获取人群包的url
    val table1="(select plan_id as planid, user_id as userid from adv.idea group by plan_id, user_id) as tmp"
    spark.read.jdbc(jdbcUrl, table1, jdbcProp).createTempView("temp_table")

    val sqlRequest5 =
      s"""
         | insert into dl_cpc.cpc_userid_all
         | select ta.userid,'$date' from (select t1.userid from temp_table t1 join
         | (select planid from dl_cpc.ml_cvr_feature_v1 where date='$date') t2 on t1.planid=t2.planid group by t1.userid) ta
         | left join dl_cpc.cpc_userid_all tb on ta.userid=tb.userid where tb.userid is null group by ta.userid
      """.stripMargin
    println(sqlRequest5)
    spark.sql(sqlRequest5)


    val sqlRequest6 =
      s"""
         | insert into dl_cpc.cpc_tag_userid_all
         | select ta.tag,ta.userid,'$date' from (select tag, userid from dl_cpc.cpc_tag_all join dl_cpc.cpc_userid_all) ta left join dl_cpc.cpc_tag_userid_all tb
         |  on ta.userid=tb.userid and ta.tag=tb.tag where tb.tag is null
      """.stripMargin

    println(sqlRequest6)
    spark.sql(sqlRequest6)
  }

}
