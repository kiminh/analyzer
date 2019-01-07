package com.cpc.spark.hottopic

import org.apache.spark.sql.SparkSession

/**
  * @author Jinbao
  * @date 2019/1/7 12:06
  */
object UserDayIntermediate {
    def main(args: Array[String]): Unit = {
        val date = args(0)

        val spark = SparkSession.builder()
          .appName(s"UserDayIntermediate date = $date")
          .enableHiveSupport()
          .getOrCreate()

        val sql =
            s"""
               |insert overwrite table dl_cpc.rdvideo_user_day_intermediate partition (dt='$date', pt='ad_behavior')
               |select
               |    final.uid as uid,
               |    concat_ws("\001", collect_list(final.content)) as content
               |from
               |    (
               |        select /*+mapjoin(BBB)*/
               |            AAA.uid,
               |            concat_ws("\002",
               |                AAA.adslot_type,
               |                AAA.ideaid,
               |                AAA.adtype,
               |                AAA.adclass,
               |                AAA.pv,
               |                AAA.click,
               |                AAA.buy,
               |                if(BBB.tokens is null, "", BBB.tokens)
               |            ) as content
               |        from
               |            (
               |                select
               |                    AA.uid, AA.adslot_type, AA.ideaid,
               |                    max(AA.adtype) as adtype,
               |                    max(AA.adclass) as adclass,
               |                    sum(AA.isshow) as pv,
               |                    sum(AA.isclick) as click,
               |                    sum(AA.isbuy) as buy
               |                from
               |                    (
               |                        select
               |                            A.*,
               |                            if(B.searchid is null, 0, 1) as isbuy
               |                        from
               |                            (
               |                                select
               |                                    searchid,
               |                                    uid,
               |                                    adslot_type, ideaid,
               |                                    adtype, adclass,
               |                                    isshow, isclick
               |                                from
               |                                    dl_cpc.slim_union_log
               |                                where
               |                                    dt='$date'
               |                                    and media_appsid in ("80002819")
               |                                    and adsrc=1
               |                                    and ideaid>0
               |                                    and isshow=1
               |                                    and uid not like "%.%"
               |                                    and uid not like "%000000%"
               |                                    and length(uid) in (14, 15, 36)
               |                            ) A
               |                        left outer join
               |                            (
               |                                select
               |                                    searchid
               |                                from
               |                                    dl_cpc.rdvideo_dl_conversion
               |                                where
               |                                    dt='$date'
               |                            ) B
               |                        on A.searchid=B.searchid
               |                    ) AA
               |                group by
               |                    AA.uid, AA.adslot_type, AA.ideaid
               |            ) AAA
               |        left outer join
               |            (
               |                select
               |                    cast(id as bigint) as ideaid,
               |                    tokens
               |                from
               |                    dl_cpc.ideaid_title
               |            ) BBB
               |        on AAA.ideaid=BBB.ideaid
               |    ) final
               |group by
               |    final.uid
             """.stripMargin

        val r = spark.sql(sql)
    }
}
