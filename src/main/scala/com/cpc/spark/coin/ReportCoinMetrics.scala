package com.cpc.spark.coin

import java.util.Properties

import com.cpc.spark.novel.OperateMySQL
import com.cpc.spark.tools.CalcMetrics
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

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

        val tmpDate = date.replace("-","")

        println(tmpDate)

        val unionSql =
            s"""
               |select ideaid,
               |    isshow,
               |    isclick,
               |    if (b.searchid is null,0,1) as label2,
               |    price,
               |    uid,
               |    userid,
               |    exp_style,
               |    is_auto_coin,
               |    exp_cvr
               |from
               |    (
               |        select searchid,ideaid,isshow,exp_style,is_auto_coin,isclick,price,uid,userid,exp_cvr
               |        from dl_cpc.cpc_basedata_union_events
               |        where day='$date'
               |        and media_appsid  in ("80000001", "80000002") and isshow = 1
               |        and ideaid > 0
               |        and adsrc = 1
               |        and city_level != 1
               |        AND (charge_type IS NULL OR charge_type = 1)
               |        and userid not in (1001028, 1501875)
               |        and adslot_id not in ("7774304","7636999","7602943","7783705","7443868","7917491","7335680","7871301")
               |        and round(adclass/1000) != 132101
               |        and adslot_type in (1,2)
               |    ) a
               |    left outer join
               |    (
               |        select tmp.searchid
               |        from
               |        (
               |            select
               |                final.searchid as searchid,
               |                final.ideaid as ideaid,
               |                case
               |                    when final.src="elds" and final.label_type=6 then 1
               |                    when final.src="feedapp" and final.label_type in (4, 5) then 1
               |                    when final.src="yysc" and final.label_type=12 then 1
               |                    when final.src="wzcp" and final.label_type in (1, 2, 3) then 1
               |                    when final.src="others" and final.label_type=6 then 1
               |                    else 0
               |                end as isreport
               |            from
               |            (
               |                select
               |                    searchid, media_appsid, uid,
               |                    planid, unitid, ideaid, adclass,
               |                    case
               |                        when (adclass like '134%' or adclass like '107%') then "elds"
               |                        when (adslot_type<>7 and adclass like '100%') then "feedapp"
               |                        when (adslot_type=7 and adclass like '100%') then "yysc"
               |                        when adclass in (110110100, 125100100) then "wzcp"
               |                        else "others"
               |                    end as src,
               |                    label_type
               |                from
               |                    dl_cpc.ml_cvr_feature_v1
               |                where
               |                    `date`='$date'
               |                    and label2=1
               |                    and media_appsid in ("80000001", "80000002")
               |                ) final
               |            ) tmp
               |        where tmp.isreport=1
               |    ) b
               |    on a.searchid = b.searchid
             """.stripMargin

        val union = spark.sql(unionSql)
        //保存到临时表里
        //union.write.mode("overwrite").saveAsTable("test.union")

        union.createOrReplaceTempView("union")

        val ideaidSql =
            s"""
               |select
               |    ideaid,
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
               |    if (nocoin_click_num!=0,round(nocoin_convert_num/nocoin_click_num, 6),0) as cvr,
               |    if (coin_click_num!=0,round(coin_convert_num/coin_click_num, 6),0) as coin_cvr,
               |    click_total_price,
               |    coin_click_total_price,
               |    uid_num,
               |    if (show_num!=0,round(click_total_price*10/show_num,6),0) as cpm,
               |    if (click_num!=0,round(click_total_price*10/click_num,6),0) as acp,
               |    if (uid_num!=0,round(click_total_price*10/uid_num,6),0) as arpu,
               |    if (uid_num!=0,round(show_num/uid_num,6),0) as aspu,
               |    if (uid_num!=0,round(convert_num*100/uid_num,6),0) as acpu,
               |    '$date' as `date`
               |from
               |(
               |    select ideaid,
               |        sum(isshow) as show_num, --展示数
               |        sum(if (isshow=1 and is_auto_coin = 1, 1, 0)) as coin_show_num, --金币展示数
               |        sum(isclick) as click_num, --点击数
               |        sum(if (isclick=1 and is_auto_coin = 1, 1, 0)) as coin_click_num, --金币点击数
               |        sum(if (isclick=1 and exp_style != 510127, 1, 0)) as nocoin_click_num, --无金币点击数
               |        sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
               |        sum(case when label2 = 1 and is_auto_coin = 1 then 1 else 0 end) as coin_convert_num, --金币样式转化数
               |        sum(case when label2 = 1 and exp_style != 510127 then 1 else 0 end ) as nocoin_convert_num, --无金币样式转化数
               |        sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
               |        sum(case WHEN isclick = 1 and is_auto_coin = 1 then price else 0 end) as coin_click_total_price, --金币点击总价
               |        count(distinct uid) as uid_num --用户数
               |    from union
               |    group by ideaid
               |) c
             """.stripMargin

        println(ideaidSql)

        val ideaidMetrics = spark.sql(ideaidSql).cache()

        ideaidMetrics.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_report_coin_ideaid_metrics")

        val conf = ConfigFactory.load()
        val mariadb_write_prop = new Properties()

        val mariadb_write_url = conf.getString("mariadb.report2_write.url")
        mariadb_write_prop.put("user", conf.getString("mariadb.report2_write.user"))
        mariadb_write_prop.put("password", conf.getString("mariadb.report2_write.password"))
        mariadb_write_prop.put("driver", conf.getString("mariadb.report2_write.driver"))

        val ideaidMetricsDelSql = s"delete from report2.report_coin_ideaid_metrics where `date` = '$date'"
        OperateMySQL.del(ideaidMetricsDelSql)
        ideaidMetrics.write.mode(SaveMode.Append)
          .jdbc(mariadb_write_url, "report2.report_coin_ideaid_metrics", mariadb_write_prop)
        println("insert into report2.report_coin_ideaid_metrics success!")
        ideaidMetrics.unpersist()

        val useridAucListSql =
            s"""
               |select cast(userid as string) as userid,exp_cvr as score,label2 as label
               |from union
             """.stripMargin

        val useridAucList = spark.sql(useridAucListSql)

        val uAuc = CalcMetrics.getGauc(spark,useridAucList,"userid")
          .select("name","auc")

        val testTable = s"test.uauc_$tmpDate"

        uAuc.write.mode("overwrite").saveAsTable(testTable)

        val useridSql =
            s"""
               |select
               |    c.userid as userid,
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
               |    '$date' as `date`
               |from
               |(
               |    select userid,
               |        sum(isshow) as show_num, --展示数
               |        sum(if (isshow=1 and is_auto_coin = 1, 1, 0)) as coin_show_num, --金币展示数
               |        sum(isclick) as click_num, --点击数
               |        sum(if (isclick=1 and is_auto_coin = 1, 1, 0)) as coin_click_num, --金币点击数
               |        sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
               |        sum(case when label2 = 1 and is_auto_coin = 1 then 1 else 0 end) as coin_convert_num, --金币样式转化数
               |        sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
               |        sum(case WHEN isclick = 1 and is_auto_coin = 1 then price else 0 end) as coin_click_total_price, --金币点击总价
               |        count(distinct uid) as uid_num --用户数
               |    from union
               |    group by userid
               |) c
             """.stripMargin

        println(useridSql)

        val useridOtherMetrics = spark.sql(useridSql)

        val testTable2 = s"test.userid_other_$tmpDate"

        useridOtherMetrics.write.mode("overwrite").saveAsTable(testTable2)

        val resultSql =
            s"""
               |select a.userid as userid,
               |  show_num,
               |  coin_show_num,
               |  coin_show_rate,
               |  click_num,
               |  coin_click_num,
               |  coin_click_rate,
               |  ctr,
               |  coin_ctr,
               |  convert_num,
               |  coin_convert_num,
               |  coin_convert_rate,
               |  cvr,
               |  coin_cvr,
               |  click_total_price,
               |  coin_click_total_price,
               |  uid_num,
               |  cpm,
               |  acp,
               |  arpu,
               |  aspu,
               |  acpu,
               |  b.auc as auc,
               |  '$date' as `date`
               |from $testTable2 left outer join
               |(
               |    select cast(name as bigint) userid,
               |           auc
               |    from $testTable
               |) b
               |on a.userid = b.userid
             """.stripMargin

        val result = spark.sql(resultSql)

        result.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_report_coin_userid_metrics")
        val useridMetricsDelSql = s"delete from report2.report_coin_userid_metrics where `date` = '$date'"
        OperateMySQL.del(useridMetricsDelSql)
        result.write.mode(SaveMode.Append)
        .jdbc(mariadb_write_url, "report2.report_coin_userid_metrics", mariadb_write_prop)
        println("insert into report2.report_coin_userid_metrics success!")

        val delSql1 = s"drop table if exists $testTable2"
        spark.sql(delSql1)
        println(s"drop table $testTable2 success!")

//        val useridSqlnoAuto =
//            s"""
//               |select
//               |    c.userid as userid,
//               |    show_num,
//               |    coin_show_num,
//               |    if (show_num!=0,round(coin_show_num/show_num, 6),0) as coin_show_rate,
//               |    click_num,
//               |    coin_click_num,
//               |    if (click_num!=0,round(coin_click_num/click_num, 6),0) as coin_click_rate,
//               |    if (show_num!=0,round(click_num/show_num, 6),0) as ctr,
//               |    if (coin_show_num!=0,round(coin_click_num/coin_show_num, 6),0) as coin_ctr,
//               |    convert_num,
//               |    coin_convert_num,
//               |    if (convert_num!=0,round(coin_convert_num/convert_num,6),0) as coin_convert_rate,
//               |    if (click_num!=0,round(convert_num/click_num, 6),0) as cvr,
//               |    if (coin_click_num!=0,round(coin_convert_num/coin_click_num, 6),0) as coin_cvr,
//               |    click_total_price,
//               |    coin_click_total_price,
//               |    uid_num,
//               |    if (show_num!=0,round(click_total_price*10/show_num,6),0) as cpm,
//               |    if (click_num!=0,round(click_total_price*10/click_num,6),0) as acp,
//               |    if (uid_num!=0,round(click_total_price*10/uid_num,6),0) as arpu,
//               |    if (uid_num!=0,round(show_num/uid_num,6),0) as aspu,
//               |    if (uid_num!=0,round(convert_num*100/uid_num,6),0) as acpu,
//               |    0.5 as auc,
//               |    '$date' as `date`
//               |from
//               |(
//               |    select userid,
//               |        sum(isshow) as show_num, --展示数
//               |        sum(if (isshow=1 and ext_int['is_auto_coin'] = 0 and ext_int['exp_style'] = 510127, 1, 0)) as coin_show_num, --金币展示数
//               |        sum(isclick) as click_num, --点击数
//               |        sum(if (isclick=1 and ext_int['is_auto_coin'] = 0 and ext_int['exp_style'] = 510127, 1, 0)) as coin_click_num, --金币点击数
//               |        sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
//               |        sum(case when label2 = 1 and ext_int['is_auto_coin'] = 0 and ext_int['exp_style'] = 510127 then 1 else 0 end) as coin_convert_num, --金币样式转化数
//               |        sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
//               |        sum(case WHEN isclick = 1 and ext_int['is_auto_coin'] = 0 and ext_int['exp_style'] = 510127 then price else 0 end) as coin_click_total_price, --金币点击总价
//               |        count(distinct uid) as uid_num --用户数
//               |    from union
//               |    group by userid
//               |) c
//             """.stripMargin
//
//        println(useridSqlnoAuto)
//
//        val useridMetricsnoAuto = spark.sql(useridSqlnoAuto).cache()
//
//        useridMetricsnoAuto.repartition(1)
//          .write
//          .mode("overwrite")
//          .insertInto("dl_cpc.cpc_report_coin_userid_metrics_no_auto")
//
//        val useridMetricsDelSqlnoAuto = s"delete from report2.report_coin_userid_metrics_no_auto where `date` = '$date'"
//        OperateMySQL.del(useridMetricsDelSqlnoAuto)
//        useridMetricsnoAuto.write.mode(SaveMode.Append)
//          .jdbc(mariadb_write_url, "report2.report_coin_userid_metrics_no_auto", mariadb_write_prop)
//        println("insert into report2.report_coin_userid_metrics_no_auto success!")
//        useridMetricsnoAuto.unpersist()
//
//        val delSql3 = s"drop table if exists $testTable"
//        spark.sql(delSql3)
//        println(s"drop table $testTable success!")

    }
    def string2Int = udf((name:String) => {
        name.toInt
    })
}

/**
  * ALTER TABLE dl_cpc.cpc_report_coin_userid_metrics ADD COLUMNS (auc double);
  */
