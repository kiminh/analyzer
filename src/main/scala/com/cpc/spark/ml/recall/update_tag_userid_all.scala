package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.SparkSession

object update_tag_userid_all {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val sqlRequest1 =
      s"""
         | insert into dl_cpc.cpc_tag_userid_all
         | select ta.profile_tag,ta.userid,'$date' from dl_cpc.cpc_userid_tag ta left join dl_cpc.cpc_tag_userid_all tb
         |  on ta.userid=tb.userid and ta.profile_tag=tb.tag where tb.tag is null
      """.stripMargin
    println(sqlRequest1)
    spark.sql(sqlRequest1)

    //    连接adv
    val jdbcProp = new Properties()
    val jdbcUrl = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    jdbcProp.put("user", "adv_live_read")
    jdbcProp.put("password", "seJzIPUc7xU")
    jdbcProp.put("driver", "com.mysql.jdbc.Driver")

    //从adv后台mysql获取人群包的url
    val table=s"(select look_like_id as tag,user_id as userid from adv.look_like where status = 0 and modified_time>'$date') as tmp"
    spark.read.jdbc(jdbcUrl, table, jdbcProp).createTempView("ftp_tag_userid")

    val sqlRequest2 =
      s"""
         | insert into dl_cpc.cpc_tag_userid_all
         | select ta.tag,ta.userid,'$date' from ftp_tag_userid ta left join dl_cpc.cpc_tag_userid_all tb
         |  on ta.userid=tb.userid and ta.tag=tb.tag where tb.tag is null
      """.stripMargin
    println(sqlRequest2)
    spark.sql(sqlRequest2)
  }

}
