package com.cpc.spark.ml.recallReport

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object userprofileCost {
  Logger.getRootLogger.setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("userprofileCost").enableHiveSupport().getOrCreate()
    val date = args(0)
    val cal = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$date"))
    cal.add(Calendar.DATE, -1)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val jdbcProp = new Properties()
    val jdbcUrl = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    jdbcProp.put("user", "adv_live_read")
    jdbcProp.put("password", "seJzIPUc7xU")
    jdbcProp.put("driver", "com.mysql.jdbc.Driver")
    val unit=
      s"""
         |(select id, tag_orient,medias from (SELECT id,
         |if(audience_orient>0 and audience_orient_filter>0, CONCAT(audience_orient,",",audience_orient_filter),
         |if(audience_orient>0,audience_orient,
         |if(audience_orient_filter>0, audience_orient_filter, null))) as tag_orient, CONCAT(media_class,target_medias) as medias FROM adv.`unit`) t
         |group by id, tag_orient, medias) temp
      """.stripMargin
    spark.read.jdbc(jdbcUrl, unit, jdbcProp).createOrReplaceTempView("table_unit")
    val cost =
      s"""
         |(select unit_id, sum(cost) as cost from adv.cost where date='$yesterday' and cost>0 group by unit_id) temp
       """.stripMargin
    spark.read.jdbc(jdbcUrl, cost, jdbcProp).createOrReplaceTempView("table_cost")

    spark.sql(
      s"""
         |select if(tb.tag_orient is not null, 'withtag', 'withouttag') as name,tb.medias, sum(cost) as totalcost
         |from table_cost ta left join table_unit tb on ta.unit_id=tb.id group by if(tb.tag_orient is not null, 'withtag', 'withouttag'), tb.medias
       """.stripMargin).createOrReplaceTempView("totalcost")
    spark.sql(
      s"""
         |select tag as name,medias, sum(cost) as totalcost from table_cost ta join (select id, tag, medias from table_unit lateral view
         |explode(split(tag_orient,',')) tag_orient as tag where tag_orient is not null group by id, tag, medias) tb on ta.unit_id=tb.id group by medias, tag
       """.stripMargin).createOrReplaceTempView("tagcost")
    spark.sql(
      s"""
         |select * from totalcost
         |union
         |select * from tagcost
       """.stripMargin).repartition(10).createOrReplaceTempView("union_table")

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.recall_report_userprofile_cost_v2 partition (day='$yesterday')
         |select * from union_table
       """.stripMargin)


  }

}
