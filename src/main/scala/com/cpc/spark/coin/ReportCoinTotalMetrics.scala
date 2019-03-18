package com.cpc.spark.coin
import java.util.Properties

import com.cpc.spark.novel.OperateMySQL
import com.cpc.spark.tools.CalcMetrics
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}


/**
  * @author Yulin
  * @date 2019/03/18 15:36
  */


object ReportCoinTotalMetrics {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val spark = SparkSession.builder()
      .appName(s"ReportCoinTotalMetrics date = $date ")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val tmpDate = date.replace("-","")

    val totalSql =
      s"""
         |select
         |    isshow,
         |    is_auto_coin,
         |    exp_style
         |    isclick,
         |    if (b.searchid is null,0,1) as label2,
         |    price,
         |    uid,
         |    tag,
         |    exp_cvr
         |from
         |    (
         |        select searchid,isshow,isclick,price,uid,exp_style,is_auto_coin,exp_cvr,
         |        case when concat_ws(',',exptags) like  '%needautocoin%'  then  '95%'  else '5%' end as tag
         |        from dl_cpc.cpc_basedata_union_events
         |        where day='$date'
         |        and media_appsid  in ("80000001", "80000002")  -- 趣头条
         |        and isshow = 1
         |        and antispam_score != 10000 -- 剔除作弊
         |        and ideaid > 0
         |        and adsrc in (1,28) --cpc广告
         |        and city_level != 1 --非一线城市
         |        and (charge_type is null or charge_type=1) --CPC计费
         |        and userid not in (1001028, 1501875) --排除测试账号
         |        and adslot_id not in ("7774304","7636999","7602943","7783705","7443868","7917491","7868332")
         |        and round(adclass/1000) != 132101 --去除 互动导流
         |        and adslot_type in (1,2) --列表页1，详情页2
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
         |                        when (adclass like '134%' or adclass like '107%') then "elds" --二类电商
         |                        when (adslot_type<>7 and adclass like '100%') then "feedapp" --feedapp
         |                        when (adslot_type=7 and adclass like '100%') then "yysc"  --应用商城
         |                        when adclass in (110110100, 125100100) then "wzcp" --网赚
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
         |        where tmp.isreport=1 --真正的转化
         |    ) b
         |    on a.searchid = b.searchid
             """.stripMargin

    val total = spark.sql(totalSql)
    total.createOrReplaceTempView("total")

    val tagAucListSql =
      s"""
         |select tag , exp_cvr as score,label2 as label
         |from total
                 """.stripMargin

    val tagAucList = spark.sql(tagAucListSql)
    val uAuc = CalcMetrics.getGauc(spark,tagAucList,"tag")
      .select("name","auc")
    val testTotalTable = s"test.uauc_total_$tmpDate"

    uAuc.write.mode("overwrite").saveAsTable(testTotalTable)

    val tagSql =
      s"""
         |select
         |    a.tag,
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
         |    if (uid_num!=0,round(convert_num*100/uid_num,6),0) as acpu, --*100是用作百分比
         |    b.auc as auc,
         |    '$date' as `date`
         |from
         |(
         |    select tag,
         |        sum(isshow) as show_num, --展示数
         |        sum(if (isshow=1 and is_auto_coin = 1, 1, 0)) as coin_show_num, --金币展示数
         |        sum(isclick) as click_num, --点击数
         |        sum(if (isclick=1 and is_auto_coin = 1, 1, 0)) as coin_click_num, --金币点击数
         |        sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
         |        sum(case when label2 = 1 and is_auto_coin = 1 then 1 else 0 end) as coin_convert_num, --金币样式转化数
         |        sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
         |        sum(case WHEN isclick = 1 and is_auto_coin = 1 then price else 0 end) as coin_click_total_price, --金币点击总价
         |        count(distinct uid) as uid_num --用户数
         |    from total
         |    group by tag
         |) a
         |left join
         |(
         | select name as tag, auc
         | from $testTotalTable
         |)b  on a.tag = b.tag
             """.stripMargin
    val result = spark.sql(tagSql)
    result.repartition(1)
      .write
      .mode("overwrite")
      .saveAsTable("test.lyl_coin_metrics")
    // .insertInto("dl_cpc.cpc_report_coin_tag_metrics")

    //val tagMetricsDelSql = s"delete from report2.cpc_report_coin_tag_metrics where `date`='$date'"
    //OperateMySQL.del(tagMetricsDelSql)
    //result.write.mode(SavaMode.Append)
     // .jdbc(mariadb_write_url,"report2.cpc_report_coin_tag_metrics",mariadb_write_prop)
    //println("insert into report2.cpc_report_coin_tag_metrics success!")
    //result.unpersist()

    val delSql = s"drop table if exists $testTotalTable"
    spark.sql(delSql)
    println(s"drop table $testTotalTable success!")
  }

}
