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
          .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
          .enableHiveSupport()
          .getOrCreate()

        import spark.implicits._

        val unionSql =
            s"""
               |select *
               |from
               |    dl_cpc.cpc_union_log
               |where
               |    `date` = '2018-11-24'
               |     and media_appsid  in ("80000001", "80000002")
               |     and isshow = 1
               |     and ext['antispam'].int_value = 0 and ideaid > 0
               |     and adsrc = 1
               |     and ext['city_level'].int_value != 1
               |     and userid not in (1001028, 1501875)
             """.stripMargin

        val union = spark.sql(unionSql)

        val featureSql =
            s"""
               |select searchid, label2
               |from dl_cpc.ml_cvr_feature_v1
               |where `date`='2018-11-24'
             """.stripMargin
        val feature = spark.sql(featureSql)

        val memberSql =
            s"""
               |select
               |    search_id as searchid,coin
               |from
               |    src_cpc.cpc_member_coin
               |where
               |    day = '2018-11-24'
             """.stripMargin
        val member =spark.sql(memberSql)

        val eval = union.join(broadcast(feature), Seq("searchid"), "left_outer")
          .join(broadcast(member),Seq("searchid"),"left_outer")

        eval.show(10)
        eval.write.mode("overwrite").saveAsTable("test.coin_evaluation")

//        val evalSql =
//            s"""
//               |select
//               |    case when exptags like '%needautocoin%' then "auto"
//               |        when exptags not like "%nostylecoin%" then "manual"
//               |        else "notag"
//               |        end as tag,
//               |    '$date' as `date`,
//               |    sum(isclick) / sum(isshow) as ctr, --点击率
//               |    sum(case when label2 = 1 then 1 else 0 end) / sum(isclick) as cvr, --转化率
//               |    sum(if (isclick=1 and coin is not null,coin,0)) click_coin_num, --金币数量
//               |    sum(if (isclick=1 and ext_int['exp_style'] =510127 and coin is not null, coin, 0)) auto_coin_num, --金币样式金币数量
//               |    round(sum(case WHEN isclick = 1 then price else 0 end)*10/sum(isshow),3) as cpm, --cpm
//               |    round(sum(case WHEN isclick = 1 then price else 0 end)*10/count(distinct uid),3) as arpu,
//               |    round(sum(case WHEN label2 = 1 then 1 else 0 end)*100/count(distinct uid),3) as acpu, --人均转化率
//               |
//               |    round(sum(case WHEN label2 = 1 and ext['usertype'].int_value = 0 then 1 else 0 end)*100/count(distinct uid),3) as acpu_0, --其他企业
//               |    round(sum(case WHEN label2 = 1 and ext['usertype'].int_value = 1 then 1 else 0 end)*100/count(distinct uid),3) as acpu_1, --非企
//               |    round(sum(case WHEN label2 = 1 and ext['usertype'].int_value = 2 then 1 else 0 end)*100/count(distinct uid),3) as acpu_2, --正企
//               |
//               |    --网赚
//               |    round(sum(if (ext['adclass'].int_value = 110110100 and isclick = 1, 1, 0)) / sum(if (ext['adclass'].int_value = 110110100 and isshow = 1, 1, 0)),5) as ctr_wangzhuan, --网赚ctr
//               |    round(sum(if (ext['adclass'].int_value = 110110100 and label2 = 1, 1, 0)) / sum(if (ext['adclass'].int_value = 110110100 and isclick = 1, 1, 0)),5) as cvr_wangzhuan, --网赚cvr
//               |    round(sum(if (ext['adclass'].int_value = 110110100 and isshow = 1, 1, 0)) * 100 / count(distinct uid),5) as aspu_wangzhuan, --网赚人均展示率*100%
//               |    round(sum(if (ext['adclass'].int_value = 110110100 and label2 = 1, 1, 0)) * 100 / count(distinct uid),5) as acpu_wangzhuan, --网赚人均转化率*100%
//               |    round(sum(if (ext['adclass'].int_value = 110110100 and isclick = 1, price, 0))*10 / count(distinct uid),5) as arpu_wangzhuan, --网赚人均价格
//               |    round(sum(if (ext['adclass'].int_value = 110110100 and isclick = 1, price, 0))*10 / sum(if (ext['adclass'].int_value = 110110100 and isclick = 1, 1, 0)), 5) as acp_wangzhuan, --网赚平均点击价格
//               |    round(sum(if (ext['adclass'].int_value = 110110100 and isclick = 1, price, 0))*10 / sum(if (ext['adclass'].int_value = 110110100 and isshow = 1, 1, 0)), 5) as cpm_wangzhuan, --网赚cpm
//               |    sum(if (ext['adclass'].int_value = 110110100 and isclick=1 and coin is not null,coin,0)) as coin_num_wangzhuan, --网赚金币数
//               |    sum(if (ext['adclass'].int_value = 110110100 and ext_int['exp_style'] =510127 and isclick=1 and coin is not null,coin,0)) as auto_coin_num_wangzhuan, --网赚金币样式金币数
//               |
//               |    --棋盘
//               |    round(sum(if (ext['adclass'].int_value = 100101109 and isclick = 1, 1, 0)) / sum(if (ext['adclass'].int_value = 100101109 and isshow = 1, 1, 0)),5) as ctr_wangzhuan, --棋盘ctr
//               |    round(sum(if (ext['adclass'].int_value = 100101109 and label2 = 1, 1, 0)) / sum(if (ext['adclass'].int_value = 100101109 and isclick = 1, 1, 0)),5) as cvr_wangzhuan, --棋盘cvr
//               |    round(sum(if (ext['adclass'].int_value = 100101109 and isshow = 1, 1, 0)) * 100 / count(distinct uid),5) as aspu_wangzhuan, --棋盘人均展示率*100%
//               |    round(sum(if (ext['adclass'].int_value = 100101109 and label2 = 1, 1, 0)) * 100 / count(distinct uid),5) as acpu_wangzhuan, --棋盘人均转化率*100%
//               |    round(sum(if (ext['adclass'].int_value = 100101109 and isclick = 1, price, 0))*10 / count(distinct uid),5) as arpu_wangzhuan, --棋盘人均价格
//               |    round(sum(if (ext['adclass'].int_value = 100101109 and isclick = 1, price, 0))*10 / sum(if (ext['adclass'].int_value = 100101109 and isclick = 1, 1, 0)), 5) as acp_wangzhuan, --棋盘平均点击价格
//               |    round(sum(if (ext['adclass'].int_value = 100101109 and isclick = 1, price, 0))*10 / sum(if (ext['adclass'].int_value = 100101109 and isshow = 1, 1, 0)), 5) as cpm_wangzhuan, --棋盘cpm
//               |    sum(if (ext['adclass'].int_value = 100101109 and isclick=1 and coin is not null,coin,0)) as coin_num_wangzhuan, --棋盘金币数
//               |    sum(if (ext['adclass'].int_value = 100101109 and ext_int['exp_style'] =510127 and isclick=1 and coin is not null,coin,0)) as auto_coin_num_wangzhuan, --棋盘金币样式金币数
//               |
//               |    --保健品
//               |    round(sum(if (ext['adclass'].int_value = 130104100 and isclick = 1, 1, 0)) / sum(if (ext['adclass'].int_value = 130104100 and isshow = 1, 1, 0)),5) as ctr_wangzhuan, --保健品ctr
//               |    round(sum(if (ext['adclass'].int_value = 130104100 and label2 = 1, 1, 0)) / sum(if (ext['adclass'].int_value = 130104100 and isclick = 1, 1, 0)),5) as cvr_wangzhuan, --保健品cvr
//               |    round(sum(if (ext['adclass'].int_value = 130104100 and isshow = 1, 1, 0)) * 100 / count(distinct uid),5) as aspu_wangzhuan, --保健品人均展示率*100%
//               |    round(sum(if (ext['adclass'].int_value = 130104100 and label2 = 1, 1, 0)) * 100 / count(distinct uid),5) as acpu_wangzhuan, --保健品人均转化率*100%
//               |    round(sum(if (ext['adclass'].int_value = 130104100 and isclick = 1, price, 0))*10 / count(distinct uid),5) as arpu_wangzhuan, --保健品人均价格
//               |    round(sum(if (ext['adclass'].int_value = 130104100 and isclick = 1, price, 0))*10 / sum(if (ext['adclass'].int_value = 130104100 and isclick = 1, 1, 0)), 5) as acp_wangzhuan, --保健品平均点击价格
//               |    round(sum(if (ext['adclass'].int_value = 130104100 and isclick = 1, price, 0))*10 / sum(if (ext['adclass'].int_value = 130104100 and isshow = 1, 1, 0)), 5) as cpm_wangzhuan, --保健品cpm
//               |    sum(if (ext['adclass'].int_value = 130104100 and isclick=1 and coin is not null,coin,0)) as coin_num_wangzhuan, --保健品金币数
//               |    sum(if (ext['adclass'].int_value = 130104100 and ext_int['exp_style'] =510127 and isclick=1 and coin is not null,coin,0)) as auto_coin_num_wangzhuan, --保健品金币样式金币数
//               |
//               |    --下载
//               |    round(sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and isclick = 1, 1, 0)) / sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and isshow = 1, 1, 0)),5) as ctr_wangzhuan, --下载ctr
//               |    round(sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and label2 = 1, 1, 0)) / sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and isclick = 1, 1, 0)),5) as cvr_wangzhuan, --下载cvr
//               |    round(sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and isshow = 1, 1, 0)) * 100 / count(distinct uid),5) as aspu_wangzhuan, --下载人均展示率*100%
//               |    round(sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and label2 = 1, 1, 0)) * 100 / count(distinct uid),5) as acpu_wangzhuan, --下载人均转化率*100%
//               |    round(sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and isclick = 1, price, 0))*10 / count(distinct uid),5) as arpu_wangzhuan, --下载人均价格
//               |    round(sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and isclick = 1, price, 0))*10 / sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and isclick = 1, 1, 0)), 5) as acp_wangzhuan, --下载平均点击价格
//               |    round(sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and isclick = 1, price, 0))*10 / sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and isshow = 1, 1, 0)), 5) as cpm_wangzhuan, --下载cpm
//               |    sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and isclick=1 and coin is not null,coin,0)) as coin_num_wangzhuan, --下载金币数
//               |    sum(if ((ext['adclass'].int_value = 100102100 or ext['adclass'].int_value = 100101100) and ext_int['exp_style'] =510127 and isclick=1 and coin is not null,coin,0)) as auto_coin_num_wangzhuan --下载金币样式金币数
//               |from test.coin_evaluation
//               |group by
//               |    case when exptags like '%needautocoin%' then "auto"
//               |        when exptags not like "%nostylecoin%" then "manual"
//               |        else "notag" end;
//             """.stripMargin
//
//        val evaluation = spark.sql(evalSql)
//
//        evaluation.show(10)
//
//        spark.sql("drop table if exists test.coin_evaluation")
//
//        println("drop table test.coin_evaluation success!")
    }
}
