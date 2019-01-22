package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/1/22 11:22
  */
object HotTopicDailyMetrics {
    def main(args: Array[String]): Unit = {
        val date = args(0)
        val spark = SparkSession.builder()
          .appName(s"HotTopicDailyMetrics date = $date")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select
               |    sum(isshow) as show_num, --展示数
               |    sum(isclick) as click_num, --点击数
               |    round(sum(isclick) / sum(isshow),6) as ctr, --点击率
               |    sum(case when label2 = 1 then 1 else 0 end) as convert_num, --转化数
               |    round(sum(case when label2 = 1 then 1 else 0 end) / sum(isclick),6) as cvr, --转化率
               |    count(distinct uid) as uid_num, --用户数
               |    sum(case WHEN isclick = 1 then price else 0 end) as click_total_price, --点击总价
               |    round(sum(case WHEN isclick = 1 then price else 0 end)*10/sum(isshow), 6) as cpm,
               |    round(sum(case WHEN isclick = 1 then price else 0 end)*10/count(distinct uid),6) as arpu,
               |    round(sum(case WHEN isclick = 1 then price else 0 end)*10/sum(isclick),6) as acp,
               |    round(sum(isshow)/count(distinct uid),6) as aspu, --人均展示率
               |    round(sum(case WHEN label2 = 1 then 1 else 0 end)*100/count(distinct uid),6) as acpu --人均转化率
               |
               |from
               |(
               |    select a.*,b.label2 as label2
               |    from
               |    (
               |        select *
               |        from dl_cpc.cpc_hot_topic_union_log
               |        where `date`='2019-01-20'
               |        and media_appsid  in ("80002819") and isshow = 1
               |        and ext['antispam'].int_value = 0 and ideaid > 0
               |        and adsrc = 1
               |        and ext['city_level'].int_value != 1
               |        AND (ext["charge_type"] IS NULL OR ext["charge_type"].int_value = 1)
               |    ) a left outer join
               |    (
               |        select searchid, label2
               |        from dl_cpc.ml_cvr_feature_v1
               |        where `date`='2019-01-20'
               |    ) b
               |    on a.searchid = b.searchid
               |) final
             """.stripMargin

        val result = spark.sql(sql)

        result.repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_hot_topic_metrics_daily")
    }
}
