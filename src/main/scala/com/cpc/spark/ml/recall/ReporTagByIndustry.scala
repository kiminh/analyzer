package com.cpc.spark.ml.recall
import java.sql.DriverManager
import java.util.Properties
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
      "left join (select id,name from adv.category) b on a.category=b.id where b.name is not null group by a.user_id,a.category,b.name ) as tmp"
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
        |	b.category1 as class_id,
        | b.name as name,
        |	a.tag as tag,
        | a.name as tag_name,
        |	cast(cast(sum(a.ctrwithtag) as string) as int) as ctrwithtag,
        |cast(cast(sum(a.ctrwithouttag) as string) as int) as ctrwithouttag,
        |	cast(sum(a.costwithtag) as double) as costwithtag,
        |	cast(sum(a.costwithouttag) as double) as costwithouttag,
        |	cast(cast(sum(a.cvrwithtag) as string) as int) as cvrwithtag,
        |cast(cast(sum(a.cvrwithouttag) as string) as int) as cvrwithouttag,
        | to_date('$date') as date
        |from
        |(
        |select userid,tag,name,ctrwithtag,ctrwithouttag,costwithtag,costwithouttag,cvrwithtag,cvrwithouttag from report_table
        |) a
        |join
        |(
        |select
        |user_id,
        |100000000 as category1,
        |case
        |when category like '100101%' then '移动游戏'
        |when category like '100102%' then '移动应用'
        |when category like '101%' then '汽车'
        |when category like '102%' then '奢侈品'
        |when category like '103%' then '教育培训'
        |when category like '104%' then '金融保险'
        |when category like '105%' then '食品饮料'
        |when category like '106%' then '家电'
        |when category like '107%' then 'IT数码'
        |when category like '108%' then '文化娱乐'
        |when category like '109%' then '旅游'
        |when category like '110%' then '互联网服务'
        |when category like '111%' then '家居建材'
        |when category like '112%' then '房地产'
        |when category like '113%' then '本地服务'
        |when category like '114%' then '日用百货'
        |when category like '115%' then '母婴儿童'
        |when category like '116%' then '体育户外'
        |when category like '117%' then '服装服饰'
        |when category like '118%' then '美容化妆'
        |when category like '119%' then '法律服务'
        |when category like '120%' then '商务服务'
        |when category like '121%' then '安全安保'
        |when category like '122%' then '营销/广告/包装'
        |when category like '123%' then '招商加盟'
        |when category like '124%' then '工农业'
        |when category like '125%' then '彩票'
        |when category like '126%' then '节能环保'
        |when category like '127%' then '婚恋交友'
        |when category like '128%' then '公益'
        |when category like '129%' then '成人用品'
        |when category like '130%' then '医疗'
        |when category like '131%' then '宠物用品'
        |when category like '132%' then '内部测试'
        |when category like '134%' then '电商购物'
        |when category like '135%' then '品牌医护类'
        |else 'null' end as name
        |from adv_table
        |) b
        |on a.userid=b.user_id
        |group by
        |	b.category1,
        | b.name,
        |	a.tag,
        | a.name
      """.stripMargin).repartition(100).
      write.mode(SaveMode.Append).jdbc(tagReport2dbUrl, "report2.cpc_profiletag_report_v1", tagReport2dbProp)

  }

  def clearReportData(date: String): Unit = {
    try {
      val conn = DriverManager.getConnection(
        tagReport2dbUrl,
        tagReport2dbProp.getProperty("user"),
        tagReport2dbProp.getProperty("password"))
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
