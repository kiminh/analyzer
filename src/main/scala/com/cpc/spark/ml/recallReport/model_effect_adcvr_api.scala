package com.cpc.spark.ml.recallReport

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.common.Murmur3Hash
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable


object model_effect_adcvr_api {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("model_effect_adcvr_api").enableHiveSupport().getOrCreate()
    val curday = args(0)
    val cal = Calendar.getInstance()
    cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(s"$curday"))
    cal.add(Calendar.DATE, -1)
    val day = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_model_effect_api partition (type='ocpc1257', day='${day}')
         |select  a.cvr_model_name,
         |            count(distinct if(a.tuid is not null,a.tuid,a.uid)) uv,
         |            sum(isshow) as imp,
         |            sum(isclick) as click,
         |            SUM(if(bid_mode = 0 AND charge_type = 2, isshow * price / 1000, if(bid_mode = 0
         |            AND charge_type != 2, isclick * price, if(bid_mode IN (4, 5)
         |            OR adsrc = 4, 0, isshow * price / 1000)))) / 100 as rev,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) as cv,
         |            sum(isclick)/sum(isshow) ctr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then raw_cvr else null end)/1000000/sum(isclick) raw_cvr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then exp_cvr else null end)/1000000/sum(isclick) exp_cvr,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end)/sum(isclick) cvr,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end)/sum(isshow) show_cvr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then raw_cvr else null end)/1000000/
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) raw_pcoc,
         |            sum(case when adsrc in (1, 28) and isclick>0 then exp_cvr else null end)/1000000/
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) exp_pcoc,
         |            sum(case when (b.searchid is not null) then cpagiven else 0 end) * 0.01 / sum(case when (b.searchid is not null)  then 1 else 0 end) as cpagiven
         |    from
         |    (select *,cast(bid_ocpc as double) as cpagiven from dl_cpc.cpc_basedata_union_events
         |     where day = '${day}'
         |        and isshow = 1
         |        AND adsrc IN (1, 28)
         |        AND conversion_goal in (2,5,7)
         |        and is_ocpc=1
         |        and cvr_model_name <> ''
         |        and media_appsid in ('80000001','80000002','80000006','80000064','80000066')
         |    ) a
         |    left join (
         |    select searchid,ideaid,conversion_goal
         |    FROM dl_cpc.ocpc_cvr_log_hourly
         |    WHERE date >= '${day}'
         |    group by searchid,ideaid,conversion_goal
         |    ) b on a.searchid=b.searchid and a.ideaid=b.ideaid and a.conversion_goal=b.conversion_goal
         |    group by a.day,a.cvr_model_name
         |""".stripMargin)

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_model_effect_api partition (type='total', day='${day}')
         |select
         |            case when num < 100 then 'qtt-cvr-dnn-rawid-v7api'
         |                 else 'qtt-cvr-dnn-rawid-v6api-reg-tf' end as tag,
         |            count(distinct a.tuid) uv,
         |            sum(isshow) as imp,
         |            sum(isclick) as click,
         |            SUM(if(bid_mode = 0 AND charge_type = 2, isshow * price / 1000, if(bid_mode = 0
         |            AND charge_type != 2, isclick * price, if(bid_mode IN (4, 5)
         |            OR adsrc = 4, 0, isshow * price / 1000)))) / 100 as rev,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) as cv,
         |            sum(isclick)/sum(isshow) ctr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then raw_cvr else null end)/1000000/sum(isclick) raw_cvr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then exp_cvr else null end)/1000000/sum(isclick) exp_cvr,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end)/sum(isclick) cvr,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end)/sum(isshow) show_cvr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then raw_cvr else null end)/1000000/
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) raw_pcoc,
         |            sum(case when adsrc in (1, 28) and isclick>0 then exp_cvr else null end)/1000000/
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) exp_pcoc,
         |            sum(case when (b.searchid is not null) then cpagiven else 0 end) * 0.01 / sum(case when (b.searchid is not null)  then 1 else 0 end) as cpagiven
         |    from
         |    (select *,if(length(tuid)>0, tuid, uid) as uid_tag, cast(bid_ocpc as double) as cpagiven from dl_cpc.cpc_basedata_union_events
         |     where day = '${day}'
         |        and isshow = 1
         |        and media_appsid in ('80000001','80000002','80000006','80000064','80000066')
         |    ) a
         |    left join (
         |    select searchid,ideaid,conversion_goal
         |    FROM dl_cpc.ocpc_cvr_log_hourly
         |    WHERE date >= '${day}'
         |    group by searchid,ideaid,conversion_goal
         |    ) b on a.searchid=b.searchid and a.ideaid=b.ideaid and a.conversion_goal=b.conversion_goal
         |    left join
         |    (select uid,num from dl_cpc.cvr_mlcpp_uid_label
         |     where dt = '${day}' and label = 276) c
         |    on a.uid_tag=c.uid
         |    group by a.day,
         |    case when num < 100 then 'qtt-cvr-dnn-rawid-v7api'
         |                 else 'qtt-cvr-dnn-rawid-v6api-reg-tf' end
         |""".stripMargin)

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_model_effect_api partition (type='ocpc5', day='${day}')
         |select  a.cvr_model_name,
         |            count(distinct if(a.tuid is not null,a.tuid,a.uid)) uv,
         |            sum(isshow) as imp,
         |            sum(isclick) as click,
         |            SUM(if(bid_mode = 0 AND charge_type = 2, isshow * price / 1000, if(bid_mode = 0
         |            AND charge_type != 2, isclick * price, if(bid_mode IN (4, 5)
         |            OR adsrc = 4, 0, isshow * price / 1000)))) / 100 as rev,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) as cv,
         |            sum(isclick)/sum(isshow) ctr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then raw_cvr else null end)/1000000/sum(isclick) raw_cvr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then exp_cvr else null end)/1000000/sum(isclick) exp_cvr,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end)/sum(isclick) cvr,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end)/sum(isshow) show_cvr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then raw_cvr else null end)/1000000/
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) raw_pcoc,
         |            sum(case when adsrc in (1, 28) and isclick>0 then exp_cvr else null end)/1000000/
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) exp_pcoc,
         |            sum(case when (b.searchid is not null) then cpagiven else 0 end) * 0.01 / sum(case when (b.searchid is not null)  then 1 else 0 end) as cpagiven
         |    from
         |    (select *,cast(bid_ocpc as double) as cpagiven from dl_cpc.cpc_basedata_union_events
         |     where day = '${day}'
         |        and isshow = 1
         |        AND adsrc IN (1, 28)
         |        AND conversion_goal in (5,7)
         |        and is_ocpc=1
         |        and cvr_model_name <> ''
         |        and media_appsid in ('80000001','80000002','80000006','80000064','80000066')
         |    ) a
         |    left join (
         |    select searchid,ideaid,conversion_goal
         |    FROM dl_cpc.ocpc_cvr_log_hourly
         |    WHERE date >= '${day}'
         |    group by searchid,ideaid,conversion_goal
         |    ) b on a.searchid=b.searchid and a.ideaid=b.ideaid and a.conversion_goal=b.conversion_goal
         |    group by a.day,a.cvr_model_name
         |""".stripMargin)

    spark.sql(
      s"""
         |insert overwrite table dl_cpc.cpc_model_effect_api partition (type='ocpc127', day='${day}')
         |select  a.cvr_model_name,
         |            count(distinct if(a.tuid is not null,a.tuid,a.uid)) uv,
         |            sum(isshow) as imp,
         |            sum(isclick) as click,
         |            SUM(if(bid_mode = 0 AND charge_type = 2, isshow * price / 1000, if(bid_mode = 0
         |            AND charge_type != 2, isclick * price, if(bid_mode IN (4, 5)
         |            OR adsrc = 4, 0, isshow * price / 1000)))) / 100 as rev,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) as cv,
         |            sum(isclick)/sum(isshow) ctr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then raw_cvr else null end)/1000000/sum(isclick) raw_cvr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then exp_cvr else null end)/1000000/sum(isclick) exp_cvr,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end)/sum(isclick) cvr,
         |            sum(case when (b.searchid is not null)  then 1 else 0 end)/sum(isshow) show_cvr,
         |            sum(case when adsrc in (1, 28) and isclick>0 then raw_cvr else null end)/1000000/
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) raw_pcoc,
         |            sum(case when adsrc in (1, 28) and isclick>0 then exp_cvr else null end)/1000000/
         |            sum(case when (b.searchid is not null)  then 1 else 0 end) exp_pcoc,
         |            sum(case when (b.searchid is not null) then cpagiven else 0 end) * 0.01 / sum(case when (b.searchid is not null)  then 1 else 0 end) as cpagiven
         |    from
         |    (select *,cast(bid_ocpc as double) as cpagiven from dl_cpc.cpc_basedata_union_events
         |     where day = '${day}'
         |        and isshow = 1
         |        AND adsrc IN (1, 28)
         |        AND conversion_goal in (1,2)
         |        and is_ocpc=1
         |        and cvr_model_name <> ''
         |        and media_appsid in ('80000001','80000002','80000006','80000064','80000066')
         |    ) a
         |    left join (
         |    select searchid,ideaid,conversion_goal
         |    FROM dl_cpc.ocpc_cvr_log_hourly
         |    WHERE date >= '${day}'
         |    group by searchid,ideaid,conversion_goal
         |    ) b on a.searchid=b.searchid and a.ideaid=b.ideaid and a.conversion_goal=b.conversion_goal
         |    group by a.day,a.cvr_model_name
         |""".stripMargin)

  }
}
