package com.cpc.spark.shortvideo

import org.apache.spark.sql.SparkSession

object rule {
  def main(args: Array[String]): Unit = {
    val date = args(0)
    val spark = SparkSession.builder()
      .appName(s"HotTopicCtrAuc date = $date")
      .enableHiveSupport()
      .getOrCreate()
    import org.apache.spark.sql._
    import spark.implicits._

    val sql =
      s"""insert overwrite table dl_cpc.cp_unionevents_appdownload_qbj partition (dt='${date}')
         | select  userid,usertype,adclass,adslot_type,adtype,interaction,media_appsid,adsrc,unitid,planid,
         |        count( events.searchid ) queries,
         |        sum(case when isshow=1 then 1 end) show_n,
         |        sum(case when isclick=1 then 1 end) click_n,
         |        sum(case when isclick=1 then 1 end) *1.0 / sum(case when isshow=1 then 1 end) ctr_ratio,
         |
         |        (sum(case WHEN isclick = 1 and (charge_type = 1 or charge_type IS NULL)  then price else 0 end)
         |      + sum(case when isshow  = 1 and  charge_type = 2   then price else 0 end)/1000.0)/100.0    as  expense,
         |        sum(case when isreport=1 then 1 end ) cvr_n,
         |        sum(case when isreport=1 then 1 end ) / sum(case when isclick=1 then 1 end)  cvr_ratio,
         |        sum(case when isclick=1 then exp_cvr *1.0 /1000000 end )  exp_cvr_n,
         |        sum(case when isclick=1 then exp_cvr *1.0 /1000000 end ) / sum(case when isclick=1 then 1 end )  exp_cvr_ratio
         | FROM
         | (
         |  select   day,searchid ,isshow,isclick,usertype,userid,adtype,interaction,adsrc,media_appsid,price,exp_cvr,charge_type
         |  from     dl_cpc.cpc_basedata_union_events
         |  where   day ='${date}'
         |  and     adsrc=1
         |  and    media_appsid in  ("80000001","80000002")
         |  and    adtype in (2,8,10)
         |  and    interaction=2
         |  and    userid>0
         |  and    usertype in (0,1,2)
         | )   events
         |
 | left JOIN
         | (   select  searchid2,ideaid,isreport,src,label_type,uid,planid,unitid, adclass,adslot_type
         |     from
         |     (     select    final.searchid as searchid2,src,label_type,uid,planid,unitid, adclass, adslot_type,label2,
         |                     final.ideaid as ideaid,
         |                     case
         |          when final.src="elds" and final.label_type=6 then 1
         |          when final.src="feedapp" and final.label_type in (4, 5) then 1
         |          when final.src="yysc" and final.label_type=12 then 1
         |          when final.src="wzcp" and final.label_type in (1, 2, 3) then 1
         |          when final.src="others" and final.label_type=6 then 1
         |          else 0     end as isreport
         |          from
         |          (       select     searchid, media_appsid, uid,
         |                       planid, unitid, ideaid, adclass,adslot_type,label2,
         |                  case
         |                  when (adclass like '134%' or adclass like '107%') then "elds"
         |                  when (adslot_type<>7 and adclass like '100%') then "feedapp"
         |                  when (adslot_type=7 and adclass like '100%') then "yysc"
         |                  when adclass in (110110100, 125100100) then "wzcp"
         |                  else "others"
         |                  end as src,
         |                  label_type
         |            from
         |              dl_cpc.ml_cvr_feature_v1
         |            where
         |              `date` ='${date}'
         |              and label2=1
         |              and media_appsid in ("80000001", "80000002")
         |            ) final
         |      ) tmp1
         |      where   tmp1.isreport=1
         | ) a
         | on  a.searchid2=events.searchid
         | group by userid,usertype,adclass,adslot_type,adtype,interaction,media_appsid,adsrc,unitid,planid

       """.stripMargin
      val  tab=spark.sql(sql).cache
      tab.repartition(100).write.mode("overwrite").insertIntoTable("dl_cpc.cp_unionevents_appdownload_qbj")

  }
}
