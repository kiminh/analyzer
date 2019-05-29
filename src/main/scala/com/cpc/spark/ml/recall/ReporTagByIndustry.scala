package com.cpc.spark.ml.recall

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

/**
  * created by xiongyao on 2019/5/29
  */
object ReporTagByIndustry {

  var tagReport2dbUrl = ""
  val tagReport2dbProp = new Properties()

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString

    //    连接adv_test
    val jdbcProp_adv = new Properties()
    val jdbcUrl_adv = "jdbc:mysql://rr-2zehhy0xn8833n2u5.mysql.rds.aliyuncs.com"
    jdbcProp_adv.put("user", "adv_live_read")
    jdbcProp_adv.put("password", "seJzIPUc7xU")
    jdbcProp_adv.put("driver", "com.mysql.jdbc.Driver")

    val adv_table="(select a.user_id as user_id,a.category as category,b.name as name from (select user_id,category from adv.idea) a " +
      "left join (select id,name from adv.category) b on a.category=b.id) as tmp"
    spark.read.jdbc(jdbcUrl_adv, adv_table, jdbcProp_adv).createTempView("adv_table")

    //  连接report2
    val jdbcProp_report = new Properties()
    val jdbcUrl_report = "jdbc:mysql://rm-2zemny6nzg818jcdn.mysql.rds.aliyuncs.com:3306/report2"
    jdbcProp_report.put("user", "report")
    jdbcProp_report.put("password", "report!@#")
    jdbcProp_report.put("driver", "com.mysql.jdbc.Driver")

    val report_table=s"(select userid,tag,name,ctrwithtag,ctrwithouttag,costwithtag,costwithouttag,cvrwithtag,cvrwithouttag from report2.cpc_profiletag_report  where date='$date' ) as tmp"
    spark.read.jdbc(jdbcUrl_report, report_table, jdbcProp_report).createTempView("report_table")

    val conf = ConfigFactory.load()
    tagReport2dbUrl = conf.getString("mariadb.report2_write.url")
    tagReport2dbProp.put("user", conf.getString("mariadb.report2_write.user"))
    tagReport2dbProp.put("password", conf.getString("mariadb.report2_write.password"))
    tagReport2dbProp.put("driver", conf.getString("mariadb.report2_write.driver"))

    spark.sql(s""" select * from adv_table limit 100 """).show(100,false)

    spark.sql(s""" select * from report_table limit 100 """).show(100,false)

    spark.sql(
      s"""
        |select
        |	b.category,
        | b.name,
        |	a.tag,
        |	sum(a.ctrwithtag),
        |	sum(a.ctrwithouttag),
        | sum(a.costwithtag),
        | sum(a.costwithouttag),
        | sum(a.cvrwithtag),
        | sum(a.cvrwithouttag)
        |from
        |(
        |select userid,tag,name,ctrwithtag,ctrwithouttag,costwithtag,costwithouttag,cvrwithtag,cvrwithouttag from report_table
        |) a
        |left join
        |(
        |select user_id,category,name from adv_table
        |) b
        |on a.userid=b.user_id
        |group by
        |	b.category,
        | b.name,
        |	a.tag
      """.stripMargin).repartition(100).show(100)
//      write.mode(SaveMode.Append).jdbc(mariaReport2dbUrl, "report2.cpc_profiletag_report", mariaReport2dbProp)




















  }

}
