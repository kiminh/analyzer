package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/2/27 11:50
  */
object BlackCoinTest {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"BlackCoinTest date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val tmpDate = date.replace("-","")
        val unionSql =
            s"""
               |select ideaid,
               |    isshow,
               |    ext_int,
               |    isclick,
               |    label2,
               |    price,
               |    uid,
               |    userid,
               |    ext
               |from
               |    (
               |        select searchid,ideaid,isshow,ext_int,isclick,price,uid,userid,ext
               |        from dl_cpc.cpc_union_log
               |        where `date`='$date'
               |        and media_appsid  in ("80000001", "80000002") and isshow = 1
               |        and ext['antispam'].int_value = 0 and ideaid > 0
               |        and adsrc = 1
               |        and ext['city_level'].int_value != 1
               |        AND (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
               |        and userid not in (1001028, 1501875)
               |        and adslotid not in ("7774304","7636999","7602943","7783705","7443868","7917491","7868332")
               |        and round(ext["adclass"].int_value/1000) != 132101
               |        and uid not in (866916038580098,862117045134896,867389025636076,867194047384543,868375036845821,868980023440039,864294033096359,869435025733112,867672039306475,864410038798910,866320031246189,863013032300518,866923035305032,866615034088912,869504030523436,28D6R17A17002060866188033250917,862393030019422,355989081352622,862468032771194,866791032561976,867229039761488,861351040368610,860391043153479,868642035499797,864878041239074,865413036601812,865910046606053,867834047856251,865240042005187,864804031403116,864082036261413,868364037291516,864588032462086,99001236034187,863119039344860,860734034084841,864244033138693)
               |        and adslot_type in (1,2)
               |    ) a
               |    left outer join
               |    (
               |        select searchid, label2
               |        from dl_cpc.ml_cvr_feature_v1
               |        where `date`='$date'
               |    ) b
               |    on a.searchid = b.searchid
             """.stripMargin

        val union = spark.sql(unionSql)
        //保存到临时表里
        //union.write.mode("overwrite").saveAsTable("test.union")

        union.createOrReplaceTempView("union")

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
               |        sum(if (isshow=1 and ext_int['is_auto_coin'] = 1, 1, 0)) as coin_show_num, --金币展示数
               |        sum(isclick) as click_num, --点击数
               |        sum(if (isclick=1 and ext_int['is_auto_coin'] = 1, 1, 0)) as coin_click_num, --金币点击数
               |        sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
               |        sum(case when label2 = 1 and ext_int['is_auto_coin'] = 1 then 1 else 0 end) as coin_convert_num, --金币样式转化数
               |        sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
               |        sum(case WHEN isclick = 1 and ext_int['is_auto_coin'] = 1 then price else 0 end) as coin_click_total_price, --金币点击总价
               |        count(distinct uid) as uid_num --用户数
               |    from union
               |    group by userid
               |) c
             """.stripMargin

        println(useridSql)

        val useridOtherMetrics = spark.sql(useridSql)

        val testTable2 = "test.userid_other_0221"

        useridOtherMetrics.write.mode("overwrite").saveAsTable(testTable2)
    }
}
