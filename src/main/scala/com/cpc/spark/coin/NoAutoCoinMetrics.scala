package com.cpc.spark.coin

import java.util.Properties

import com.cpc.spark.novel.OperateMySQL
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @author Jinbao
  * @date 2019/4/25 14:57
  */
object NoAutoCoinMetrics {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"NoAutoCoinMetrics date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val unionSql =
            s"""
               |select userid, ideaid, isshow, isclick, price, uid, is_auto_coin, exp_cvr, exp_style,
               | if (b.searchid is null,0,1) as label2
               |from
               |(
               |    select searchid, userid, ideaid, isshow, isclick, price, uid, is_auto_coin, exp_cvr, exp_style
               |    from dl_cpc.cpc_basedata_union_events
               |    where day = '$date'
               |    and media_appsid in ("80000001", "80000002", "80000006", "80000064", "80000066")
               |    and isshow = 1
               |    and ideaid > 0
               |    and userid > 0
               |    and adsrc in (0, 1, 28)
               |    and (charge_type is null or charge_type=1)
               |) a
               |left outer join
               |(
               |    select tmp.searchid
               |    from
               |    (
               |        select final.searchid as searchid, final.ideaid as ideaid,
               |            case when final.src="elds" and final.label_type=6 then 1
               |                when final.src="feedapp" and final.label_type in (4, 5) then 1
               |                when final.src="yysc" and final.label_type=12 then 1
               |                when final.src="wzcp" and final.label_type in (1, 2, 3) then 1
               |                when final.src="others" and final.label_type=6 then 1
               |                else 0
               |            end as isreport
               |        from
               |        (
               |            select searchid, media_appsid, uid, planid, unitid, ideaid, adclass,
               |                case
               |                    when (adclass like '134%' or adclass like '107%') then "elds"
               |                    when (adslot_type<>7 and adclass like '100%') then "feedapp"
               |                    when (adslot_type=7 and adclass like '100%') then "yysc"
               |                    when adclass in (110110100, 125100100) then "wzcp"
               |                    else "others"
               |                end as src,
               |                label_type
               |            from dl_cpc.ml_cvr_feature_v1
               |            where `date`='$date'
               |                and label2=1
               |                and media_appsid in ("80000001", "80000002")
               |            ) final
               |        ) tmp
               |    where tmp.isreport=1
               |) b
               |on a.searchid = b.searchid
             """.stripMargin

        val union = spark.sql(unionSql)

        union.createOrReplaceTempView("union")

        val useridSql =
            s"""
               |select userid,
               |    if (show_num is null,0,show_num) as show_num,
               |    if (coin_show_num is null,0,coin_show_num) as coin_show_num,
               |    if (show_num>0,round(coin_show_num/show_num, 6),0) as coin_show_rate,
               |    if (click_num is null,0,click_num) as click_num,
               |    if (coin_click_num is null,0,coin_click_num) as coin_click_num,
               |    if (click_num>0,round(coin_click_num/click_num, 6),0) as coin_click_rate,
               |    if (show_num>0,round(click_num/show_num, 6),0) as ctr,
               |    if (coin_show_num>0,round(coin_click_num/coin_show_num, 6),0) as coin_ctr,
               |    if (convert_num is null,0,convert_num) as convert_num,
               |    if (coin_convert_num is null,0,coin_convert_num) as coin_convert_num,
               |    if (convert_num>0,round(coin_convert_num/convert_num,6),0) as coin_convert_rate,
               |    if (nocoin_click_num>0,round(nocoin_convert_num/nocoin_click_num, 6),0) as cvr,
               |    if (coin_click_num>0,round(coin_convert_num/coin_click_num, 6),0) as coin_cvr,
               |    if (click_total_price is null,0,click_total_price) as click_total_price,
               |    if (coin_click_total_price is null,0,coin_click_total_price) as coin_click_total_price,
               |    if (uid_num is null,0,uid_num) as uid_num,
               |    if (show_num>0,round(click_total_price*10/show_num,6),0) as cpm,
               |    if (click_num>0,round(click_total_price*10/click_num,6),0) as acp,
               |    if (uid_num>0,round(click_total_price*10/uid_num,6),0) as arpu,
               |    if (uid_num>0,round(show_num/uid_num,6),0) as aspu,
               |    if (uid_num>0,round(convert_num*100/uid_num,6),0) as acpu,
               |    0 as auc,
               |    '$date' as `date`
               |from
               |(
               |    select userid,
               |        sum(isshow) as show_num, --展示数
               |        sum(if (isshow=1 and exp_style = 510127 and is_auto_coin = 0, 1, 0)) as coin_show_num, --金币展示数
               |        sum(isclick) as click_num, --点击数
               |        sum(if (isclick=1 and exp_style = 510127 and is_auto_coin = 0, 1, 0)) as coin_click_num, --金币点击数
               |        sum(if (isclick=1 and exp_style != 510127, 1, 0)) as nocoin_click_num, --无金币点击数
               |        sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
               |        sum(case when label2 = 1 and exp_style = 510127 and  is_auto_coin = 0 then 1 else 0 end) as coin_convert_num, --金币样式转化数
               |        sum(case when label2 = 1 and exp_style != 510127 then 1 else 0 end ) as nocoin_convert_num, --无金币样式转化数
               |        sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
               |        sum(case WHEN isclick = 1 and exp_style = 510127 and is_auto_coin = 0 then price else 0 end) as coin_click_total_price, --金币点击总价
               |        count(distinct uid) as uid_num --用户数
               |    from union
               |    group by userid
               |) final
             """.stripMargin

        val useridMetrics = spark.sql(useridSql).na.fill(0,Seq("ctr"))   //计算ideaid级别的指标

        useridMetrics.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_report_coin_userid_metrics_no_auto")

        val conf = ConfigFactory.load() //读取配置文件中mysql的数据
        val mariadb_write_prop = new Properties()
        val mariadb_write_url = conf.getString("mariadb.report2_write.url")
        mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
        mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
        mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

        val useridMetricsDelSqlnoAuto = s"delete from report2.report_coin_userid_metrics_no_auto where `date` = '$date'"
        OperateMySQL.del(useridMetricsDelSqlnoAuto)
        useridMetrics.write.mode(SaveMode.Append)
          .jdbc(mariadb_write_url, "report2.report_coin_userid_metrics_no_auto", mariadb_write_prop)
        println("insert into report2.report_coin_userid_metrics_no_auto success!")

    }
}
