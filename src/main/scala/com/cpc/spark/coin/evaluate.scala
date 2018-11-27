package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
  * @author Jinbao
  * @date 2018/11/26 16:10
  */
object evaluate {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"evaluate date = $date")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        val unionSql =
            s"""
               |select
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
               |    sum(case WHEN label2 = 1 and ext['usertype'].int_value = 0 then 1 else 0 end), --其他企业数
               |    sum(case WHEN label2 = 1 and ext['usertype'].int_value = 1 then 1 else 0 end), --非企
               |    sum(case WHEN label2 = 1 and ext['usertype'].int_value = 2 then 1 else 0 end), --正企
               |
               |    --网赚
               |    sum(if (round(ext['adclass'].int_value/1000) = 110110 and isshow = 1, 1, 0)) as show_wangzhuan, --网赚展示数
               |    sum(if (round(ext['adclass'].int_value/1000) = 110110 and isclick = 1, 1, 0)) as click_wangzhuan, --网赚点击数
               |    sum(if (round(ext['adclass'].int_value/1000) = 110110 and label2 = 1, 1, 0)) as convert_wangzhuan, --网赚转化数
               |    sum(if (round(ext['adclass'].int_value/1000) = 110110 and isclick = 1, price, 0)) as click_price_wangzhuan, --网赚点击总价
               |    sum(if (round(ext['adclass'].int_value/1000) = 110110 and isclick = 1 and ext_int['exp_style'] =510127, 1, 0)) as click_num_wangzhuan, --网赚金币点击数
               |
               |    --下载
               |    sum(if (round(ext['adclass'].int_value/1000) = 100102 and isshow = 1, 1, 0)) as show_xiazai, --下载展示数
               |    sum(if (round(ext['adclass'].int_value/1000) = 100102 and isclick = 1, 1, 0)) as click_xiazai, --下载点击数
               |    sum(if (round(ext['adclass'].int_value/1000) = 100102 and label2 = 1, 1, 0)) as convert_xiazai, --下载转化数
               |    sum(if (round(ext['adclass'].int_value/1000) = 100102 and isclick = 1, price, 0)) as click_price_xiazai, --下载点击总价
               |    sum(if (round(ext['adclass'].int_value/1000) = 100102 and isclick = 1 and ext_int['exp_style'] =510127, 1, 0)) as click_num_xiazai, --下载金币点击数
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
               |    from (
               |        select *
               |        from
               |            dl_cpc.cpc_union_log
               |        where
               |            `date` = '2018-11-26'
               |            and media_appsid  in ("80000001", "80000002")
               |            and isshow = 1
               |            and ext['antispam'].int_value = 0 and ideaid > 0
               |            and adsrc = 1
               |            and ext['city_level'].int_value != 1
               |            and userid not in (1001028, 1501875)
               |    ) a
               |    left outer join
               |    (
               |        select searchid, label2
               |        from dl_cpc.ml_cvr_feature_v1
               |        where `date`='2018-11-26'
               |    ) b
               |    on a.searchid = b.searchid
               |
               |    group by
               |        case when a.exptags like '%needautocoin%' then "auto"
               |            when a.exptags not like "%nostylecoin%" then "manual"
               |            else "notag" end;
             """.stripMargin

        val union = spark.sql(unionSql)

        union.show(10)
    }
}
