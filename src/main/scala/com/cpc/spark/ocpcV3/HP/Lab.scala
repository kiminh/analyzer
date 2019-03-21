package com.cpc.spark.ocpcV3.HP

import java.util.Calendar
import java.text.SimpleDateFormat
import org.apache.spark.sql.{SparkSession, DataFrame}

object Lab {
  def main(args: Array[String]): Unit ={
    val spark = SparkSession.builder().appName("videoPromotion").enableHiveSupport().getOrCreate()

    val date = args(0).toString
    val hour = args(1).toString
    val hr = args(2).toString
    val baseData = getBaseData(spark, date, hour, hr)
    baseData.persist()

    baseData.write.mode("overwrite").saveAsTable("test.baseData2_sjq")
    println("========================baseData=======================")
    baseData.show(20)

  }

  def getBaseData(spark: SparkSession, date: String, hour: String, hr: String)= {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, 1)
    val tomorrow = calendar.getTime
    val date1 = dateConverter.format( tomorrow )
    val timeCondition1 = s"(( day   = '$date' and hour >= '$hour') or ( day   = '$date1' and hour < '$hour'))"
    val timeCondition2 = s"((`date` = '$date' and hour >= '$hour') or (`date` = '$date1' and hour < '$hour'))"
    println("time1: " + timeCondition1)
    println("time2: " + timeCondition2)

    val sql1 =
      s"""
         |select
         |  case
         |    when exptags like '%use_strategy%' then 'A'
         |    else 'B'
         |  end as test_tag,
         |  t1.searchid,
         |  uid,
         |  cast(adclass/1000 as int) as adclass2,
         |  t1.userid,
         |  case when tt.userid is not NULL then 1 else 0 end as if_use_strategy,
         |  tt.threshold,
         |  usertype,
         |  adtype1,
         |  ideaid,
         |  isshow,
         |  isclick,
         |  charge_type,
         |  price,
         |  t2.iscvr,
         |  exp_cvr
         |from
         |  (
         |    select
         |      searchid,
         |      concat_ws(',', exptags) as exptags,
         |      uid,
         |      adclass,
         |      userid,
         |      usertype,
         |      case
         |        when adtype = 2 then 'bigimage'
         |        else 'video'
         |      end as adtype1,
         |      ideaid,
         |      isshow,
         |      isclick,
         |      charge_type,
         |      price,
         |      exp_cvr
         |    from
         |      dl_cpc.cpc_basedata_union_events
         |    where
         |      $timeCondition1
         |      and adsrc = 1
         |      --and isclick = 1
         |      --and isshow = 1
         |      and media_appsid in ("80000001")
         |      and adtype in (2, 8, 10)
         |      and usertype in (0, 1, 2)
         |      and adslot_type = 1
         |      --and (charge_type is NULL or charge_type = 1)
         |      and ideaid > 0
         |      and  interaction=2
         |      and userid > 0
         |      --  and uid not like "%.%"
         |      --  and uid not like "%000000%"
         |      --  and length(uid) in (14, 15, 36)
         |
         |  ) t1
         |  left join (  select userid, expcvr as threshold from dl_cpc.cpc_appdown_cvr_threshold  where dt = '$date' and hr = '$hr' group by userid, expcvr ) tt
         |    on t1.userid = tt.userid
         |  left join (
         |    select
         |      tmp.searchid,
         |      1 as iscvr
         |    from
         |      (
         |        select
         |          final.searchid as searchid,
         |          final.ideaid as ideaid,
         |          case
         |            when final.src = "elds"
         |            and final.label_type = 6 then 1
         |            when final.src = "feedapp"
         |            and final.label_type in (4, 5) then 1
         |            when final.src = "yysc"
         |            and final.label_type = 12 then 1
         |            when final.src = "wzcp"
         |            and final.label_type in (1, 2, 3) then 1
         |            when final.src = "others"
         |            and final.label_type = 6 then 1
         |            else 0
         |          end as isreport
         |        from
         |          (
         |            select
         |              searchid,
         |              media_appsid,
         |              uid,
         |              planid,
         |              unitid,
         |              ideaid,
         |              adclass,
         |              case
         |                when (
         |                  adclass like '134%'
         |                  or adclass like '107%'
         |                ) then "elds" -- 二类电商
         |                when (
         |                  adslot_type <> 7
         |                  and adclass like '100%'
         |                ) then "feedapp"
         |                when (
         |                  adslot_type = 7
         |                  and adclass like '100%'
         |                ) then "yysc" --应用商场
         |                when adclass in (110110100, 125100100) then "wzcp" --网赚彩票（110110100：网赚, 125100100：彩票）
         |                else "others"
         |              end as src,
         |              label_type
         |            from
         |              dl_cpc.ml_cvr_feature_v1
         |            where
         |              $timeCondition2
         |              and label2 = 1
         |              and media_appsid in ("80000001", "80000002")
         |          ) final
         |      ) tmp
         |    where
         |      tmp.isreport = 1
         |  ) t2 on t1.searchid = t2.searchid
       """.stripMargin
    println(sql1)

    val result = spark.sql(sql1).na.fill(0, Seq("iscvr"))
    result
  }

}
