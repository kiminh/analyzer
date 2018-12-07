package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018/11/28 11:29
  */
object EvaluationAutoCoin {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"EvaluationAutoCoin date = $date")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        val sql =
            s"""
               |select a.*,b.label2 as label2
               |from
               |(
               |    select *
               |    from dl_cpc.cpc_union_log
               |    where `date`='$date'
               |    and media_appsid  in ("80000001", "80000002") and isshow = 1
               |    and ext['antispam'].int_value = 0 and ideaid > 0
               |    and adsrc = 1
               |    and ext['city_level'].int_value != 1
               |    AND (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
               |    and userid not in (1001028, 1501875)
               |    and adslotid not in ("7774304","7636999","7602943","7783705")
               |    and adslot_type in (1,2)
               |) a left outer join
               |(
               |    select searchid, label2
               |    from dl_cpc.ml_cvr_feature_v1
               |    where `date`='$date'
               |) b
               |on a.searchid = b.searchid
             """.stripMargin
        println(sql)
        val union = spark.sql(sql)
        val testTable = "test.union_feature"
        union.write.mode("overwrite").saveAsTable(testTable)

        val ideaidSql =
            s"""
               |select
               |    '$date' as `date`,
               |    ideaid,
               |    sum(isclick) as click_num,
               |    sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then 1 else 0 end) as coin_click_num,
               |    round(sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then 1 else 0 end)/sum(case WHEN isshow = 1 and ext_int['exp_style'] =510127 then 1 else 0 end),6) as coin_ctr,
               |    round(sum(case WHEN label2 = 1 and ext_int['exp_style'] =510127 then 1 else 0 end)/sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then 1 else 0 end),6) as coin_cvr,
               |    sum(if (isclick = 1 , price, 0)) click_price,
               |    sum(if (isclick = 1 and ext_int['exp_style'] =510127 , price, 0)) auto_coin_price
               |from $testTable
               |group by ideaid
             """.stripMargin
        println(ideaidSql)
        val ideaidEvaluation = spark.sql(ideaidSql)

        ideaidEvaluation.show(5)

        ideaidEvaluation.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.auto_coin_evaluation_ideaid_daily")
        println("insert into dl_cpc.auto_coin_evaluation_ideaid_daily success!")

        val useridSql =
            s"""
               |select
               |    '$date' as `date`,
               |    userid,
               |    sum(isshow) as show_num,
               |    sum(case WHEN isshow = 1 and ext_int['exp_style'] =510127 then 1 else 0 end) as coin_show_num,
               |    sum(isclick) / sum(isshow) as ctr,
               |    sum(case when label2 = 1 then 1 else 0 end) / sum(isclick) as cvr,
               |    round(sum(case WHEN isclick = 1 then price else 0 end)*10/sum(isshow),3) as cpm,
               |    round(sum(case WHEN isclick = 1 then price else 0 end)*10/count(distinct uid),3) as arpu,
               |    round(sum(case WHEN label2 = 1 then 1 else 0 end)*100/count(distinct uid),3) as acpu,
               |    round(sum(isshow)/count(distinct uid),3) as aspu,
               |    sum(isclick) as click_num,
               |    sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then 1 else 0 end) as coin_click_num,
               |    round(sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then 1 else 0 end) / sum(isclick),6) as coin_percent
               |from $testTable
               |group by userid
             """.stripMargin
        println(useridSql)

        val useridEvaluation = spark.sql(useridSql)

        useridEvaluation.show(5)

        useridEvaluation.repartition(1)
            .write
            .mode("overwrite")
            .insertInto("dl_cpc.auto_coin_evaluation_userid_daily")
        println("insert into dl_cpc.auto_coin_evaluation_userid_daily success!")

        val summarySql =
            s"""
               |select tag,
               |    click_num,
               |    show_num,
               |    convert_num,
               |    coin_click_num,
               |    click_price,
               |    uid_num,
               |    convert_0,
               |    convert_1,
               |    convert_2,
               |    round(click_num/show_num,6) as ctr,
               |
               |    round(convert_num/click_num,6) as cvr,
               |    round(click_price*10/show_num,6) as cpm,
               |    round(click_price*10/uid_num,6) as arpu,
               |    round(show_num/uid_num,6) as aspu,
               |
               |    round(convert_num*100/uid_num,6) as acpu,
               |
               |    round(show_0/uid_num,6) as aspu_0,
               |    round(show_1/uid_num,6) as aspu_1,
               |    round(show_2/uid_num,6) as aspu_2,
               |
               |    round(click_price_0*10/uid_num,6) as arpu_0,
               |    round(click_price_1*10/uid_num,6) as arpu_1,
               |    round(click_price_2*10/uid_num,6) as arpu_2,
               |
               |    round(convert_0*100/uid_num,6) as acpu_0,
               |    round(convert_1*100/uid_num,6) as acpu_1,
               |    round(convert_2*100/uid_num,6) as acpu_2,
               |
               |    show_wangzhuan,
               |    click_wangzhuan,
               |    convert_wangzhuan,
               |    click_price_wangzhuan,
               |    round(click_wangzhuan/show_wangzhuan,6) as ctr_wangzhuan,
               |    round(convert_wangzhuan/click_wangzhuan,6) as cvr_wangzhuan,
               |    round(show_wangzhuan/uid_num,6) as aspu_wangzhuan,
               |    round(convert_wangzhuan/uid_num,6) as acpu_wangzhuan,
               |    round(click_price_wangzhuan*10/uid_num,6) as arpu_wangzhuan,
               |    round(click_price_wangzhuan*10/click_wangzhuan,6) as acp_wangzhuan,
               |    round(click_price_wangzhuan*10/show_wangzhuan,6) as cpm_wangzhuan,
               |    click_num_wangzhuan,
               |
               |    show_xiazai,
               |    click_xiazai,
               |    convert_xiazai,
               |    click_price_xiazai,
               |    round(click_xiazai/show_xiazai,6) as ctr_xiazai,
               |    round(convert_xiazai/click_xiazai,6) as cvr_xiazai,
               |    round(show_xiazai/uid_num,6) as aspu_xiazai,
               |    round(convert_xiazai/uid_num,6) as acpu_xiazai,
               |    round(click_price_xiazai*10/uid_num,6) as arpu_xiazai,
               |    round(click_price_xiazai*10/click_xiazai,6) as acp_xiazai,
               |    round(click_price_xiazai*10/show_xiazai,6) as cpm_xiazai,
               |    click_num_xiazai,
               |
               |    show_baojianpin,
               |    click_baojianpin,
               |    convert_baojianpin,
               |    click_price_baojianpin,
               |    round(click_baojianpin/show_baojianpin,6) as ctr_baojianpin,
               |    round(convert_baojianpin/click_baojianpin,6) as cvr_baojianpin,
               |    round(show_baojianpin/uid_num,6) as aspu_baojianpin,
               |    round(convert_baojianpin/uid_num,6) as acpu_baojianpin,
               |    round(click_price_baojianpin*10/uid_num,6) as arpu_baojianpin,
               |    round(click_price_baojianpin*10/click_baojianpin,6) as acp_baojianpin,
               |    round(click_price_baojianpin*10/show_baojianpin,6) as cpm_baojianpin,
               |    click_num_baojianpin,
               |
               |    show_qipai,
               |    click_qipai,
               |    convert_qipai,
               |    click_price_qipai,
               |    round(click_qipai/show_qipai,6) as ctr_qipai,
               |    round(convert_qipai/click_qipai,6) as cvr_qipai,
               |    round(show_qipai/uid_num,6) as aspu_qipai,
               |    round(convert_qipai/uid_num,6) as acpu_qipai,
               |    round(click_price_qipai*10/uid_num,6) as arpu_qipai,
               |    round(click_price_qipai*10/click_qipai,6) as acp_qipai,
               |    round(click_price_qipai*10/show_qipai,6) as cpm_qipai,
               |    click_num_qipai,
               |
               |    '$date' as `date`
               |
               |from (
               |    select
               |    case when a.exptags like '%needautocoin%' then "auto"
               |        when a.exptags not like "%nostylecoin%" then "manual"
               |        else "notag"
               |        end as tag,
               |    sum(isclick) as click_num, --点击数
               |    sum(isshow) as show_num, --展示数
               |    sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
               |    sum(if (isclick=1 and ext_int['exp_style'] =510127, 1, 0)) as coin_click_num, --金币点击数
               |
               |    sum(case WHEN isclick = 1 then price else 0 end) as click_price, --点击总价
               |    count(distinct uid) as uid_num, --用户数
               |
               |    sum(case WHEN isclick = 1 and ext['usertype'].int_value = 0 then price else 0 end) as click_price_0,
               |    sum(case WHEN isclick = 1 and ext['usertype'].int_value = 1 then price else 0 end) as click_price_1,
               |    sum(case WHEN isclick = 1 and ext['usertype'].int_value = 2 then price else 0 end) as click_price_2,
               |
               |    sum(case WHEN ext['usertype'].int_value = 0 then 1 else 0 end) as show_0,
               |    sum(case WHEN ext['usertype'].int_value = 1 then 1 else 0 end) as show_1,
               |    sum(case WHEN ext['usertype'].int_value = 2 then 1 else 0 end) as show_2,
               |
               |    sum(case WHEN label2 = 1 and ext['usertype'].int_value = 0 then 1 else 0 end) as convert_0, --其他企业数
               |    sum(case WHEN label2 = 1 and ext['usertype'].int_value = 1 then 1 else 0 end) as convert_1, --非企
               |    sum(case WHEN label2 = 1 and ext['usertype'].int_value = 2 then 1 else 0 end) as convert_2, --正 企
               |
               |    --网赚
               |    sum(if (round(ext['adclass'].int_value/1000) = 110110 and isshow = 1, 1, 0)) as show_wangzhuan, --网赚展示数
               |    sum(if (round(ext['adclass'].int_value/1000) = 110110 and isclick = 1, 1, 0)) as click_wangzhuan, --网赚点击数
               |    sum(if (round(ext['adclass'].int_value/1000) = 110110 and label2 = 1, 1, 0)) as convert_wangzhuan, --网赚转化数
               |    sum(if (round(ext['adclass'].int_value/1000) = 110110 and isclick = 1, price, 0)) as click_price_wangzhuan, --网赚点击总价
               |    sum(if (round(ext['adclass'].int_value/1000) = 110110 and isclick = 1 and ext_int['exp_style'] =510127, 1, 0)) as click_num_wangzhuan, --网赚金币点击数
               |
               |    --下载
               |    sum(if (round(ext['usertype'].int_value = 2 and interaction=2 and isshow = 1, 1, 0)) as show_xiazai, --下载展示数
               |    sum(if (round(ext['usertype'].int_value = 2 and interaction=2 and isclick = 1, 1, 0)) as click_xiazai, --下载点击数
               |    sum(if (round(ext['usertype'].int_value = 2 and interaction=2 and label2 = 1, 1, 0)) as convert_xiazai, --下载转化数
               |    sum(if (round(ext['usertype'].int_value = 2 and interaction=2 and isclick = 1, price, 0)) as click_price_xiazai, --下载点击总价
               |    sum(if (round(ext['usertype'].int_value = 2 and interaction=2 and isclick = 1 and ext_int['exp_style'] =510127, 1, 0)) as click_num_xiazai, --下载金币点击数
               |
               |    --保健品
               |    sum(if (round(ext['adclass'].int_value/1000) = 130104 and isshow = 1, 1, 0)) as show_baojianpin, --保健品展示数
               |    sum(if (round(ext['adclass'].int_value/1000) = 130104 and isclick = 1, 1, 0)) as click_baojianpin, --保健品点击数
               |    sum(if (round(ext['adclass'].int_value/1000) = 130104 and label2 = 1, 1, 0)) as convert_baojianpin, --保健品转化数
               |    sum(if (round(ext['adclass'].int_value/1000) = 130104 and isclick = 1, price, 0)) as click_price_baojianpin, --保健品点击总价
               |    sum(if (round(ext['adclass'].int_value/1000) = 130104 and isclick = 1 and ext_int['exp_style'] =510127, 1, 0)) as click_num_baojianpin, --保健品金币点击数
               |
               |    --棋牌
               |    sum(if (ext['adclass'].int_value = 100101109 and isshow = 1, 1, 0)) as show_qipai, --棋牌展示数
               |    sum(if (ext['adclass'].int_value = 100101109 and isclick = 1, 1, 0)) as click_qipai, --棋牌点击数
               |    sum(if (ext['adclass'].int_value = 100101109 and label2 = 1, 1, 0)) as convert_qipai, --棋牌转化数
               |    sum(if (ext['adclass'].int_value = 100101109 and isclick = 1, price, 0)) as click_price_qipai, --棋牌点击总价
               |    sum(if (ext['adclass'].int_value = 100101109 and isclick = 1 and ext_int['exp_style'] =510127, 1, 0)) as click_num_qipai --棋牌金币点击数
               |
               |    from $testTable a
               |    group by
               |        case when a.exptags like '%needautocoin%' then "auto"
               |            when a.exptags not like "%nostylecoin%" then "manual"
               |            else "notag" end
               |)
             """.stripMargin
        println(summarySql)

        val summary = spark.sql(summarySql)

        summary.show(5)

        summary.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.auto_coin_evaluation_daily")

        println("insert into dl_cpc.auto_coin_evaluation_daily success!")

//        spark.sql(s"drop table if exists $testTable")
//        println(s"drop $testTable success!")
    }
}
