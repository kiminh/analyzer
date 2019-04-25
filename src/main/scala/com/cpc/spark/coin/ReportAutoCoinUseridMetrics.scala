package com.cpc.spark.coin

import java.util.Properties

import com.cpc.spark.novel.OperateMySQL
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author Jinbao
  * @date 2019/3/15 14:08
  */
object ReportAutoCoinUseridMetrics {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"ReportAutoCoinUseridMetrics date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select
               |    a.userid as userid,
               |    show_num,
               |    coin_show_num,
               |    if (show_num!=0,round(coin_show_num/show_num, 6),0) as coin_show_rate,
               |    click_num,
               |    coin_click_num,
               |    if (click_num!=0,round(coin_click_num/click_num, 6),0) as coin_click_rate,
               |    if (show_num!=0,round(click_num/show_num, 6),0) as ctr,
               |    if (coin_show_num!=0,round(coin_click_num/coin_show_num, 6),0) as coin_ctr,
               |    convert_num,
               |    coin_convert_num,
               |    if (convert_num!=0,round(coin_convert_num/convert_num,6),0) as coin_convert_rate,
               |    if (click_num!=0,round(convert_num/click_num, 6),0) as cvr,
               |    if (coin_click_num!=0,round(coin_convert_num/coin_click_num, 6),0) as coin_cvr,
               |    click_total_price,
               |    coin_click_total_price,
               |    uid_num,
               |    if (show_num!=0,round(click_total_price*10/show_num,6),0) as cpm,
               |    if (click_num!=0,round(click_total_price*10/click_num,6),0) as acp,
               |    if (uid_num!=0,round(click_total_price*10/uid_num,6),0) as arpu,
               |    if (uid_num!=0,round(show_num/uid_num,6),0) as aspu,
               |    if (uid_num!=0,round(convert_num*100/uid_num,6),0) as acpu,
               |    auc,
               |    '$date' as `date`
               |from
               |(
               |    select userid,
               |        sum(show_num) as show_num,
               |        sum(coin_show_num) as coin_show_num,
               |        sum(click_num) as click_num,
               |        sum(coin_click_num) as coin_click_num,
               |        sum(convert_num) as convert_num,
               |        sum(coin_convert_num) as coin_convert_num,
               |        sum(click_total_price) as click_total_price,
               |        sum(coin_click_total_price) as coin_click_total_price,
               |        sum(uid_num) as uid_num
               |    from dl_cpc.cpc_report_auto_coin_ideaid_metrics
               |    where `date`='$date'
               |    group by userid
               |) a
               |left join
               |(
               |    select userid, max(auc) as auc
               |    from dl_cpc.cpc_coin_userid_auc
               |    where `date` = '$date'
               |    group by userid
               |) b
               |on a.userid = b.userid
             """.stripMargin

        val useridMetrics = spark.sql(sql)

        useridMetrics.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_report_coin_userid_metrics")

        val conf = ConfigFactory.load() //读取配置文件中mysql的数据
        val mariadb_write_prop = new Properties()

        val mariadb_write_url = conf.getString("mariadb.report2_write.url")
        mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
        mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
        mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

        val useridMetricsDelSql = s"delete from report2.report_coin_userid_metrics where `date` = '$date'"
        OperateMySQL.del(useridMetricsDelSql)
        useridMetrics.write.mode(SaveMode.Append)
          .jdbc(mariadb_write_url, "report2.report_coin_userid_metrics", mariadb_write_prop)
        println("insert into report2.report_coin_userid_metrics success!")
    }
}
