package com.cpc.spark.ml.recall

import java.sql.DriverManager
import java.util.Properties

import com.cpc.spark.ml.recall.report_userprofile_effect.{mariaReport2dbProp, mariaReport2dbUrl}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

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

    spark.sql(s""" select * from adv_table limit 100 """).show(10,false)

    spark.sql(s""" select * from report_table limit 100 """).show(10,false)

    clearReportData(date)

    spark.sql(
      s"""
        |select
        |	cast(coalesce(b.category,0) as int) as class_id,
        | coalesce(b.name,'Unknown') as name,
        |	cast(coalesce(a.tag,0) as int) as tag,
        |	sum(a.ctrwithtag) as ctrwithtag,
        | sum(a.ctrwithouttag) as ctrwithouttag,
        |	cast(coalesce(sum(a.costwithtag),0) as int) as costwithtag,
        |	cast(coalesce(sum(a.costwithouttag),0) as int) as costwithouttag,
        |	cast(coalesce(sum(a.cvrwithtag),0) as int) as cvrwithtag,
        |	cast(coalesce(sum(a.cvrwithouttag),0) as int) as cvrwithouttag,
        | to_date('$date') as date
        |from
        |(
        |select userid,tag,name,ctrwithtag,ctrwithouttag,costwithtag,costwithouttag,cvrwithtag,cvrwithouttag from report_table
        |) a
        |join
        |(
        |select user_id,category,name from adv_table
        |) b
        |on a.userid=b.user_id
        |group by
        |	b.category,
        | b.name,
        |	a.tag
      """.stripMargin).repartition(100).filter{x=> { x.getAs("class_id")==132101100 && (x.getAs[Int]("ctrwithtag") < 0 || x.getAs[Int]("ctrwithouttag") < 0)}}.show(100,false)
//      write.mode(SaveMode.Append).jdbc(tagReport2dbUrl, "report2.cpc_profiletag_report_v1", tagReport2dbProp)

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
          |delete from report2.cpc_profiletag_report_v1 where `date` = "%s"
        """.stripMargin.format(date)
      stmt.executeUpdate(sql);
    } catch {
      case e: Exception => println("exception caught: " + e);
    }
  }
}
