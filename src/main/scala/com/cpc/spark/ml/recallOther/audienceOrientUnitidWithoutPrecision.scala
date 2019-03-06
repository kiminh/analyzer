package com.cpc.spark.ml.recallOther

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.SparkSession

object audienceOrientUnitidWithoutPrecision {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("audienceOrientUnitidWithoutPrecision")
      .enableHiveSupport()
      .getOrCreate()
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val tardate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
    //连接adv后台，从mysql中获取ideaid的相关信息
    val jdbcProp = new Properties()
    val jdbcUrl = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    jdbcProp.put("user", "adv_live_read")
    jdbcProp.put("password", "seJzIPUc7xU")
    jdbcProp.put("driver", "com.mysql.jdbc.Driver")

    //从adv后台mysql获取人群包的url
    /**
    val table="(select user_id as userid, adslot_type, type as adtype, clk_site_id as site_id, category as adclass from adv.idea where status=0 and audit=1) as tmp"
    val idea = spark.read.jdbc(jdbcUrl, table, jdbcProp).distinct()
    idea.printSchema()

    idea.show(5)
      */
    val precision=
      s"""
         |(select id as unitid, audience_orient,precition_tag from adv.unit ta
         |left join (select tc.user_id,tc.look_like_id as precition_tag from adv.look_like tc
         |left join
         |adv.look_like_package td on tc.look_like_id=td.look_like_id where tc.type=2 and tc.status=0
         |and td.status in (1,2,3) group by tc.user_id,tc.look_like_id) tb on ta.user_id=tb.user_id
         |where audience_orient>0) temp
      """.stripMargin
    spark.read.jdbc(jdbcUrl, precision, jdbcProp).createOrReplaceTempView("precision")

    spark.sql(
      s"""
         |select distinct unitid from precision lateral view explode(split(audience_orient,',')) audience_orient as tag
         |where tag in (select distinct precition_tag from precision where precition_tag is not null) or tag in ('297')
       """.stripMargin
    ).repartition(1).write.text(s"/home/cpc/dgd/data/unitid_$tardate")//.createOrReplaceTempView("precision_unit")

//    val adv=
//      s"""
//         |(select cast(id as CHAR) as unitid from
//         |(SELECT unit_id FROM adv.cost where cost>0 and date>='$tardate' group by unit_id) ta
//         |join adv.unit tb on ta.unit_id=tb.id
//         |where audience_orient>0) temp
//      """.stripMargin
    //spark.read.jdbc(jdbcUrl, adv, jdbcProp).distinct().repartition(1).write.text(s"/home/cpc/dgd/data/unitid_$tardate")
}
}
