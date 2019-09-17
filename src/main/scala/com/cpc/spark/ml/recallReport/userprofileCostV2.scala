package com.cpc.spark.ml.recallReport

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object userprofileCostV2 {
  Logger.getRootLogger.setLevel(Level.WARN)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("userprofileCost").enableHiveSupport().getOrCreate()
    val date = args(0)
    val cal = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$date"))
    cal.add(Calendar.DATE, -1)
    val yesterday = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val advDBProp = new Properties()
    val advDBUrl = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    advDBProp.put("user", "adv_live_read")
    advDBProp.put("password", "seJzIPUc7xU")
    advDBProp.put("driver", "com.mysql.jdbc.Driver")

    val dmpDBProp = new Properties()
    val dmpDBUrl = "jdbc:mysql://rm-2zetr56052n7m13t3.mysql.rds.aliyuncs.com"
    dmpDBProp.put("user", "dmp_w")
    dmpDBProp.put("password", "Eqi0Zt04MP5vxnFT")
    dmpDBProp.put("driver", "com.mysql.jdbc.Driver")

    val unit=
      s"""
         |(
         |  SELECT
         |    id,
         |    tag_orient,
         |    medias
         |  FROM
         |    (
         |      SELECT
         |        id,
         |        IF (
         |          audience_orient > 0 AND audience_orient_filter > 0,
         |          CONCAT(audience_orient, ",", audience_orient_filter),
         |          IF (
         |            audience_orient > 0,
         |            audience_orient,
         |            IF (
         |              audience_orient_filter > 0,
         |              audience_orient_filter,
         |              null
         |            )
         |          )
         |        ) AS tag_orient,
         |        CONCAT(media_class, target_medias) AS medias
         |      FROM
         |        adv.`unit`
         |    ) t
         |  GROUP BY
         |    id,
         |    tag_orient,
         |    medias
         |) temp
      """.stripMargin
    spark.read.jdbc(advDBUrl, unit, advDBProp).createOrReplaceTempView("table_unit")
    val cost =
      s"""
         |(
         |  select
         |    unit_id,
         |    sum(cost) as cost
         |  from
         |    adv.cost
         |  where
         |    date = '$yesterday'
         |    and cost > 0
         |  group by
         |    unit_id
         |) temp
       """.stripMargin
    spark.read.jdbc(advDBUrl, cost, advDBProp).createOrReplaceTempView("table_cost")

    spark.sql(
      s"""
         |select
         |  if(
         |    tb.tag_orient is not null,
         |    'withtag',
         |    'withouttag'
         |  ) as name,
         |  tb.medias,
         |  sum(cost) as totalcost
         |from
         |  table_cost ta
         |  left join table_unit tb on ta.unit_id = tb.id
         |group by
         |  if(
         |    tb.tag_orient is not null,
         |    'withtag',
         |    'withouttag'
         |  ),
         |  tb.medias
       """.stripMargin).createOrReplaceTempView("totalcost")
    spark.sql(
      s"""
         |select
         |  tag as name,
         |  medias,
         |  sum(cost) as totalcost
         |from
         |  table_cost ta
         |  join (
         |    select
         |      id,
         |      tag,
         |      medias
         |    from
         |      table_unit lateral view explode(split(tag_orient, ',')) tag_orient as tag
         |    where
         |      tag_orient is not null
         |    group by
         |      id,
         |      tag,
         |      medias
         |  ) tb on ta.unit_id = tb.id
         |group by
         |  medias,
         |  tag
       """.stripMargin).createOrReplaceTempView("tagcost")
    spark.sql(
      s"""
         |select * from totalcost
         |union
         |select * from tagcost
       """.stripMargin).repartition(10).createOrReplaceTempView("union_table")

    val dmp =
      s"""
         |(select group_id, create_type from dmp.user_group_info group by group_id, create_type) temp_lookalike
       """.stripMargin
    spark.read.jdbc(dmpDBUrl, dmp, dmpDBProp).createOrReplaceTempView("table_lookalike")
    spark.sql(
      s"""
         |select t1.*,t2.name as tagname, COALESCE(t3.count, t4.count) as count, t5.type1 from
         |(select * from union_table) t1
         |left join
         |(select tag,count(distinct uid) as count from dl_cpc.cpc_userprofile_tag_hourly where date='$yesterday' group by tag) t3
         |on t1.name=t3.tag
         |left join
         |(select tag,count(distinct uid) as count from dl_cpc.cpc_userprofile_tag_daily where date='$yesterday' group by tag) t4
         |on t1.name=t4.tag
         |left join
         |(select look_like_id, if(type=1, 'lookalike', 'precision') as type1 from table_lookalike) t5
         |on t1.name=t5.look_like_id
       """.stripMargin).repartition(5).createOrReplaceTempView("result_table")

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.recall_report_userprofile_cost_v3 partition (day='$yesterday')
         |select * from result_table
       """.stripMargin)
  }

}
