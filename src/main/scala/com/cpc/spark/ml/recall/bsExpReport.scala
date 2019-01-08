package com.cpc.spark.ml.recall

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import org.apache.spark.sql.SparkSession

object bsExpReport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("bsExp report")
      .enableHiveSupport()
      .getOrCreate()
    val jdbcProp = new Properties()
    val jdbcUrl = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    jdbcProp.put("user", "adv_live_read")
    jdbcProp.put("password", "seJzIPUc7xU")
    jdbcProp.put("driver", "com.mysql.jdbc.Driver")

    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val table2=
      s"""
         |(select id as unitid, user_id as userid, plan_id as planid, adslot_type, charge_type from
         |(SELECT unit_id,SUM(cost) as cnt FROM adv.cost where cost>0 and date='$day' group by unit_id) ta
         |join adv.unit tb on ta.unit_id=tb.id where adslot_type=1 and audience_orient>0 order by cnt desc limit 100) temp
      """.stripMargin
    spark.read.jdbc(jdbcUrl, table2, jdbcProp).select("unitid").createTempView("unitid_table")
    val table3=
      s"""insert into dl_cpc.cpc_recall_bsExp_unitid partition (`date`='$day')
         |select unitid from unitid_table
      """.stripMargin
    spark.sql(table3)
  }


}
