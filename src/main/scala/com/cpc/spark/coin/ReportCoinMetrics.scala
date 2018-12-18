package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018/12/17 20:33
  */
object ReportCoinMetrics {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"ReportCoinMetrics date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val useridSql =
            s"""
               |select
               |    userid,
               |    show_num,
               |    coin_show_num,
               |    round(coin_show_num/show_num, 6) as coin_show_rate,
               |    click_num,
               |    coin_click_num,
               |    round(coin_click_num/click_num, 6) as coin_click_rate,
               |    round(click_num/show_num, 6) as ctr,
               |    round(coin_click_num/coin_show_num, 6) as coin_ctr,
               |    convert_num,
               |    coin_convert_num,
               |    round(coin_convert_num/convert_num,6) as coin_convert_rate,
               |    round(convert_num/click_num, 6) as cvr,
               |    round(coin_convert_num/coin_click_num, 6) as coin_cvr,
               |    click_total_price,
               |    coin_click_total_price,
               |    uid_num,
               |    round(click_total_price*10/show_num,6) as cpm,
               |    round(click_total_price*10/click_num,6) as acp,
               |    round(click_total_price*10/uid_num,6) as arpu,
               |    round(show_num/uid_num,6) as aspu,
               |    round(convert_num*100/uid_num,6) as acpu,
               |    '$date' as `date`
               |from
               |(
               |    select userid,
               |        sum(isshow) as show_num, --展示数
               |        sum(if (isshow=1 and ext_int['exp_style'] = 510127, 1, 0)) as coin_show_num, --金币展示数
               |        sum(isclick) as click_num, --点击数
               |        sum(if (isclick=1 and ext_int['exp_style'] =510127, 1, 0)) as coin_click_num, --金币点击数
               |        sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
               |        sum(case when label2 = 1 and ext_int['exp_style'] = 510127 then 1 else 0 end) as coin_convert_num, --金币样式转化数
               |        sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
               |        sum(case WHEN isclick = 1 and ext_int['exp_style'] = 510127 then price else 0 end) as coin_click_total_price, --金币点击总价
               |        count(distinct uid) as uid_num --用户数
               |    from
               |    (
               |        select *
               |        from dl_cpc.cpc_union_log
               |        where `date`='$date'
               |        and media_appsid  in ("80000001", "80000002") and isshow = 1
               |        and ext['antispam'].int_value = 0 and ideaid > 0
               |        and adsrc = 1
               |        and ext['city_level'].int_value != 1
               |        AND (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
               |        and userid not in (1001028, 1501875)
               |        and adslotid not in ("7774304","7636999","7602943","7783705")
               |        and adslot_type in (1,2)
               |    ) a
               |    left outer join
               |    (
               |        select searchid, label2
               |        from dl_cpc.ml_cvr_feature_v1
               |        where `date`='$date'
               |    ) b
               |    on a.searchid = b.searchid
               |    group by userid
               |) c
             """.stripMargin

        val useridMetrics = spark.sql(useridSql)

        useridMetrics.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_report_coin_userid_metrics")

        val ideaidSql =
            s"""
               |select
               |    ideaid,
               |    show_num,
               |    coin_show_num,
               |    round(coin_show_num/show_num, 6) as coin_show_rate,
               |    click_num,
               |    coin_click_num,
               |    round(coin_click_num/click_num, 6) as coin_click_rate,
               |    round(click_num/show_num, 6) as ctr,
               |    round(coin_click_num/coin_show_num, 6) as coin_ctr,
               |    convert_num,
               |    coin_convert_num,
               |    round(coin_convert_num/convert_num,6) as coin_convert_rate,
               |    round(convert_num/click_num, 6) as cvr,
               |    round(coin_convert_num/coin_click_num, 6) as coin_cvr,
               |    click_total_price,
               |    coin_click_total_price,
               |    uid_num,
               |    round(click_total_price*10/show_num,6) as cpm,
               |    round(click_total_price*10/click_num,6) as acp,
               |    round(click_total_price*10/uid_num,6) as arpu,
               |    round(show_num/uid_num,6) as aspu,
               |    round(convert_num*100/uid_num,6) as acpu,
               |    '$date' as `date`
               |from
               |(
               |    select ideaid,
               |        sum(isshow) as show_num, --展示数
               |        sum(if (isshow=1 and ext_int['exp_style'] = 510127, 1, 0)) as coin_show_num, --金币展示数
               |        sum(isclick) as click_num, --点击数
               |        sum(if (isclick=1 and ext_int['exp_style'] =510127, 1, 0)) as coin_click_num, --金币点击数
               |        sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
               |        sum(case when label2 = 1 and ext_int['exp_style'] = 510127 then 1 else 0 end) as coin_convert_num, --金币样式转化数
               |        sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
               |        sum(case WHEN isclick = 1 and ext_int['exp_style'] = 510127 then price else 0 end) as coin_click_total_price, --金币点击总价
               |        count(distinct uid) as uid_num --用户数
               |    from
               |    (
               |        select *
               |        from dl_cpc.cpc_union_log
               |        where `date`='$date'
               |        and media_appsid  in ("80000001", "80000002") and isshow = 1
               |        and ext['antispam'].int_value = 0 and ideaid > 0
               |        and adsrc = 1
               |        and ext['city_level'].int_value != 1
               |        AND (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
               |        and userid not in (1001028, 1501875)
               |        and adslotid not in ("7774304","7636999","7602943","7783705")
               |        and adslot_type in (1,2)
               |    ) a
               |    left outer join
               |    (
               |        select searchid, label2
               |        from dl_cpc.ml_cvr_feature_v1
               |        where `date`='$date'
               |    ) b
               |    on a.searchid = b.searchid
               |    group by ideaid
               |) c
             """.stripMargin

        val ideaidMetrics = spark.sql(ideaidSql)

        ideaidMetrics.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_report_coin_ideaid_metrics")
    }
}
