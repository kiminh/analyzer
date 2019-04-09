package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
/**
  * @author Jinbao
  * @date 2019/3/29 10:44
  */
object HotTopicAdMetrics {
    def main(args: Array[String]): Unit = {
        val day = args(0)
        val spark = SparkSession.builder()
          .appName(s"HotTopicAdMetrics day = $day")
          .enableHiveSupport()
          .getOrCreate()
        import spark.implicits._

        val sql =
            s"""
               |select ideaid,adclass,adclass2,adclass1,
               |    sum(isshow) as show_num,
               |    sum(isclick) as click_num,
               |    sum(if(b.searchid is not null,1,0)) as convert_num
               |from
               |(
               |    select searchid,ideaid,isshow,isclick,
               |        adclass,
               |        cast(adclass/1000 as int) as adclass2,
               |        cast(adclass/1000000 as int) as adclass1
               |    from dl_cpc.cpc_basedata_union_events
               |    where day = '$day'
               |    and media_appsid in ('80002819')
               |    and isshow = 1
               |    and ideaid > 0
               |    and uid not like "%.%"
               |    and uid not like "%000000%"
               |    and length(uid) in (14, 15, 36)
               |) a
               |left outer join
               |(
               |     select tmp.searchid
               |     from
               |     (
               |         select final.searchid as searchid, final.ideaid as ideaid,
               |              case when final.src="elds" and final.label_type=6 then 1
               |                   when final.src="feedapp" and final.label_type in (4, 5) then 1
               |                   when final.src="yysc" and final.label_type=12 then 1
               |                   when final.src="wzcp" and final.label_type in (1, 2, 3) then 1
               |                   when final.src="others" and final.label_type=6 then 1
               |                   else 0
               |              end as isreport
               |        from
               |        (
               |            select searchid, media_appsid, uid, planid, unitid, ideaid, adclass,
               |                   case when (adclass like '134%' or adclass like '107%') then "elds"
               |                        when (adslot_type<>7 and adclass like '100%') then "feedapp"
               |                        when (adslot_type=7 and adclass like '100%') then "yysc"
               |                        when adclass in (110110100, 125100100) then "wzcp"
               |                        else "others"
               |                  end as src,
               |                  label_type
               |            from
               |                dl_cpc.ml_cvr_feature_v1
               |            where
               |                `date`='$day'
               |                and label2=1
               |                and media_appsid in ("80002819")
               |        ) final
               |      ) tmp
               |      where tmp.isreport=1
               |) b
               |on a.searchid = b.searchid
               |group by ideaid,adclass,adclass2,adclass1
             """.stripMargin

        spark.sql(sql).repartition(1)
          .write
          .mode("overwrite")
          .insertInto("dl_cpc.cpc_hot_topic_ad_metrics")

    }
    def getShowClickRank() = udf((show_num:Int) => {
        if (show_num < 1000)
            1
        else if (show_num < 5000)
            2
        else if (show_num < 10000)
            3
        else if (show_num < 50000)
            4
        else if (show_num < 100000)
            5
        else
            0
    })
    def getCtrCvrRank() = udf((ctr:Double) => {
        val ctr_rank = ( ctr * 100 ).toInt
        ctr_rank
    })
}

/*
CREATE TABLE IF NOT EXISTS dl_cpc.cpc_hot_topic_ad_metrics
(
    ideaid int,
    adclass int,
    adclass2 int,
    adclass1 int,
    show_rank int,
    click_rank int,
    ctr_rank int,
    cvr_rank int
)
PARTITIONED BY (`dt` string)
STORED AS PARQUET;
 */
