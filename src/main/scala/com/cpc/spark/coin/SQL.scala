package com.cpc.spark.coin

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2018/12/5 15:11
  */
object SQL {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
          .appName("Spark SQL")
          .enableHiveSupport()
          .getOrCreate()

        val sql =
            s"""
               |SELECT
               |case when a.ext_string["exp_ids"] like '%7168%' then "notag"
               |when a.ext_string["exp_ids"] like '%7169%' then "nostylecoin"
               |when a.ext_string["exp_ids"] like '%7170%' then "nostylecoin_needautocoin"
               |when a.ext_string["exp_ids"] like '%7171%' then "needautocoin"
               |else "error" end,
               |sum(isshow) as show_num,
               |sum(isclick) as click_num,
               |sum(isclick)/sum(isshow) as ctr,
               |sum(if (isshow=1 and ext_int['exp_style'] =510127, 1, 0)) as coin_show_num,
               |sum(if (isclick=1 and ext_int['exp_style'] =510127, 1, 0)) as coin_click_num,
               |
               |sum(case WHEN isclick = 1 then price else 0 end) as click_price, --点击总价
               |
               |count(distinct uid) as uid_num, --用户数
               |
               |round(sum(case WHEN isclick = 1 then price else 0 end)*10/sum(isshow), 6) as cpm,
               |
               |round(sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then price else 0 end)*10/sum(case when isshow=1 and ext_int['exp_style'] =510127 then 1 else 0 end), 6) as coin_cpm,
               |
               |round(sum(case WHEN isclick = 1 then price else 0 end)*10/count(distinct uid),6) as arpu,
               |
               |round(sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then price else 0 end)*10/count(distinct uid),6) as arpu,
               |
               |round(sum(if (isclick=1 and ext_int['exp_style'] =510127, 1, 0)) / sum(if (isshow=1 and ext_int['exp_style'] =510127, 1, 0)) , 6) as coin_ctr,
               |
               |sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then price else 0 end) as coin_price,
               |
               |round(sum(case WHEN isclick = 1 and ext_int['exp_style'] =510127 then price else 0 end)*10 / sum(if (isclick=1 and ext_int['exp_style'] =510127, 1, 0)), 6) as coin_acp,
               |
               |sum(case WHEN isshow=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 0 then 1 else 0 end) as show_0, --其他企业数
               |sum(case WHEN isshow=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 1 then 1 else 0 end) as show_1, --非企
               |sum(case WHEN isshow=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 2 then 1 else 0 end) as show_2, --正 企
               |sum(case WHEN isclick=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 0 then 1 else 0 end) as click_0, --其他企业数
               |sum(case WHEN isclick=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 1 then 1 else 0 end) as click_1, --非企
               |sum(case WHEN isclick=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 2 then 1 else 0 end) as click_2, --正 企，
               |
               |sum(case WHEN isclick=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 0 then 1 else 0 end)/sum(case WHEN isshow=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 0 then 1 else 0 end) as ctr_0, --其他企业数
               |sum(case WHEN isclick=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 1 then 1 else 0 end)/sum(case WHEN isshow=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 1 then 1 else 0 end) as ctr_1, --非企
               |sum(case WHEN isclick=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 2 then 1 else 0 end)/sum(case WHEN isshow=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 2 then 1 else 0 end) as ctr_2, --正企
               |
               |sum(case WHEN isclick=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 0 then price else 0 end)*10/sum(case WHEN isshow=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 0 then 1 else 0 end) as cpm_0, --其他企业数
               |sum(case WHEN isclick=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 1 then price else 0 end)*10/sum(case WHEN isshow=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 1 then 1 else 0 end) as cpm_1, --非企
               |sum(case WHEN isclick=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 2 then price else 0 end)*10/sum(case WHEN isshow=1 and ext_int['exp_style'] =510127 and ext['usertype'].int_value = 2 then 1 else 0 end) as cpm_2 --正企
               |
               |FROM test.test_coin_1204 a
               |WHERE media_appsid in ("80000001", "80000002") and isshow = 1
               |and ext['antispam'].int_value = 0 and ideaid > 0
               |and adsrc = 1
               |and ext['city_level'].int_value != 1
               |AND (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
               |----and userid not in (1001028, 1501875)
               |and adslotid not in ("7774304","7636999","7602943","7783705")
               |and adslot_type in (1,2)
               |GROUP BY
               |case when a.ext_string["exp_ids"] like '%7168%' then "notag"
               |when a.ext_string["exp_ids"] like '%7169%' then "nostylecoin"
               |when a.ext_string["exp_ids"] like '%7170%' then "nostylecoin_needautocoin"
               |when a.ext_string["exp_ids"] like '%7171%' then "needautocoin"
               |else "error" end
             """.stripMargin

        val all = spark.sql(sql)

        all.show(10)
    }
}
