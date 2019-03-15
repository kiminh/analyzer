package com.cpc.spark.ml.recallReport

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.SparkSession

object bscvr_early_warning {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bscvr early warning")
      .enableHiveSupport()
      .getOrCreate()
    val cal1 = Calendar.getInstance()
    cal1.add(Calendar.DATE, -1)
    val oneday = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    cal1.add(Calendar.DATE, -1)
    val twodays = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    cal1.add(Calendar.DATE, -1)
    val threedays = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    cal1.add(Calendar.DATE, -2)
    val fivedays = new SimpleDateFormat("yyyy-MM-dd").format(cal1.getTime)
    spark.sql(
      s"""
         |select distinct unitid from dl_cpc.cpc_recall_high_confidence_unitid where date='$oneday'
       """.stripMargin).createOrReplaceTempView("unitid_table")
    //连接adv后台，从mysql中获取unitid的相关信息
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
    //1万以内波动超过30%，1-10万波动超过20%，10万以上超过10%
    val unitid_cost=
      s"""
         |(select unit_id, user_id, date, cost from
         |(select unit_id, date, sum(cost) as cost from adv.cost where (date between '$twodays' and '$oneday')
         |and cost>0 group by unit_id, date) ta
         |left join (select id, user_id from adv.unit group by id, user_id) tb
         |on ta.unit_id=tb.id) temp
      """.stripMargin
    spark.read.jdbc(jdbcUrl, unitid_cost, jdbcProp).repartition(1).createOrReplaceTempView("unitid_cost_table")

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_recall_bscvr_early_warning partition (date='$oneday')
         |select ta.unit_id, ta.user_id, tb.cost, (coalesce(tb.cost,0)-coalesce(ta.cost,0))*1.0/coalesce(ta.cost,1) as percent
         |(select * from unitid_cost_table where date='$twodays' and unit_id in (select unitid from unitid_table)) ta
         |left join
         |(select * from unitid_cost_table where date='$oneday' and unit_id in (select unitid from unitid_table)) tb
         |on ta.unit_id=tb.unit_id
       """.stripMargin
    )

//    spark.sql(
//      s"""
//         |select unitid,userid,cost,percent from dl_cpc.cpc_recall_bscvr_early_warning where date='$oneday'
//         |and ((cost*1.0/100<10000 and percent<-0.3) or (cost*1.0/100<100000 and cost*1.0/100>10000 and percent<-0.2)
//         |or (cost*1.0/100<1000000 and cost*1.0/100>100000 and percent<-0.1))
//       """.stripMargin
//    ).createOrReplaceTempView("unitid_change")
  }
}
