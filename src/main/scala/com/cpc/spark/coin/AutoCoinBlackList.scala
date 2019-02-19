package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/2/14 20:37
  */
object AutoCoinBlackList {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"AutoCoinBlackList date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select uid
               |from
               |(
               |  select uid,
               |    sum(if (isshow=1 and ext_int['is_auto_coin'] = 1, 1, 0)) as coin_show_num,
               |    sum(if (isclick=1 and ext_int['is_auto_coin'] = 1, 1, 0)) as coin_click_num,
               |    sum(case when label2 = 1 and ext_int['is_auto_coin'] = 1 then 1 else 0 end) as coin_convert_num
               |  from
               |  (
               |    select searchid,isshow,ext_int,isclick,uid
               |    from dl_cpc.coin_union_log
               |    where `date` <= '$date' and `date`> date_sub('$date',7)
               |    and media_appsid  in ("80000001", "80000002") and isshow = 1
               |    and ext['antispam'].int_value = 0 and ideaid > 0
               |    and adsrc = 1
               |    and ext['city_level'].int_value != 1
               |    AND (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
               |    and userid not in (1001028, 1501875)
               |    and adslotid not in ("7774304","7636999","7602943","7783705","7443868","7917491","7868332")
               |    and round(ext["adclass"].int_value/1000) != 132101
               |    and adslot_type in (1,2)
               |  ) a
               |  left outer join
               |  (
               |    select searchid, label2
               |    from dl_cpc.ml_cvr_feature_v1
               |    where `date` <= '$date' and `date`> date_sub('$date',7)
               |  ) b
               |  on a.searchid = b.searchid
               |  group by uid
               |) x
               |where x.coin_click_num > 14 and x.coin_convert_num = 0
             """.stripMargin
    }
}
