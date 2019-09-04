package com.cpc.spark.ml.cvrmodel.daily

import com.cpc.spark.ml.dnn.Utils.DateUtils
import com.cpc.spark.ml.train.LRIRModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.SparkSession

/**
  * @author fym
  * @version created: 2019-05-28 10:53
  * @desc
  */
object LREval {

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val spark = SparkSession.builder().appName("LREval").enableHiveSupport().getOrCreate()

    import spark.implicits._

    val date = args(0)
    val tomorrow=DateUtils.getPrevDate(date, -1)

    val beforeSql=
      """
        |create temporary function auc as 'hivemall.evaluation.AUCUDAF' using jar "/home/cpc/anal/lib/hivemall-all-0.5.2-incubating.jar"
      """.stripMargin
    spark.sql(beforeSql)

//    val as_sql =
//      s"""
//         |select
//         |cvr_model_name,
//         |auc(raw_cvr, coalesce(label,0)) as auc,
//         |avg(raw_cvr)*sum(isclick)/sum(label) as pcoc
//         |from
//         |(
//         |select cvr_model_name, isshow, isclick, raw_cvr, exptags,case when cv_types = null then 0
//         |           when conversion_goal = 1 and B.cv_types like '%cvr1%' then 1
//         |           when conversion_goal = 2 and B.cv_types like '%cvr2%' then 1
//         |           when conversion_goal = 3 and B.cv_types like '%cvr3%' then 1
//         |           when conversion_goal = 4 and B.cv_types like '%cvr4%' then 1
//         |           when conversion_goal = 0 and is_api_callback = 1 and B.cv_types like '%cvr2%' then 1
//         |           when conversion_goal = 0 and is_api_callback = 0 and (adclass like '11011%' or adclass like '125%') and B.cv_types like '%cvr4%' then 1
//         |           when conversion_goal = 0 and is_api_callback = 0 and adclass not like '11011%' and adclass not like '125%' and B.cv_types like '%cvr%' then 1
//         |      else 0 end as label from
//         |  (select
//         |  searchid,conversion_goal,is_api_callback,adclass,if(array_contains(exptags, 'bslrcvr=bs-v4-cvr') and array_contains(exptags, 'bscvrfactor=0'), 1, 0) as cvr_model_name, isshow, isclick, raw_cvr*1.0/1000000 as raw_cvr, bsrawcvr*1.0/1000000 as bsrawcvr, exptags
//         |  from
//         |  dl_cpc.cpc_basedata_union_events
//         |  where
//         |  day in ('$date')
//         |  and isshow=0
//         |  and isclick=0
//         |  and media_appsid in ('80000001','80000002')
//         |  and adsrc=1
//         |  and charge_type = 1
//         |  and array_contains(exptags, 'bslradtypecorrection'))A
//         |  left outer join
//         |   (
//         |      select
//         |      searchid, concat_ws(',', collect_set(cvr_goal)) as cv_types
//         |      from
//         |         dl_cpc.ocpc_label_cvr_hourly
//         |      where
//         |         `date`>="$date" and `date`<="$tomorrow"
//         |      and label=1
//         |      group by searchid
//         |   ) B
//         |   on A.searchid=B.searchid
//         |  distribute by cvr_model_name sort by raw_cvr DESC
//         |) t1
//         |group by cvr_model_name
//      """.stripMargin
//    println("as_sql="+as_sql)
//    val as_df=spark.sql(as_sql)
//    println("============= as_df ===============")
//    as_df.show(5)


    val bs_sql =
      s"""
         |select
         |    AAA.conversion_goal,
         |    sum(AAA.isclick) as click,
         |    sum(AAA.isbuy) as cv,
         |    avg(AAA.bs_cvr) as pre_cvr,
         |    sum(AAA.isbuy) / sum(AAA.isclick) as post_cvr,
         |    auc(AAA.bs_cvr, AAA.isbuy) as auc,
         |    avg(abs(AAA.bs_cvr - AAA.isbuy)) as mae,
         |    avg(AAA.bs_cvr) / (sum(AAA.isbuy) / sum(AAA.isclick)) as pcoc
         |from
         |    (
         |        select
         |            *
         |        from
         |        (
         |            select
         |                A.media_tag as media_tag,
         |                A.conversion_goal as conversion_goal,
         |                A.hour as hour,
         |                A.cvr_model_name as cvr_model_name,
         |                A.searchid as searchid,
         |                A.userid as userid,
         |                A.unitid as unitid,
         |                A.ideaid as ideaid,
         |                A.isclick as isclick,
         |                A.bs_cvr as bs_cvr,
         |                case
         |                    when conversion_goal = 1 and b.cv_types like '%cvr1%' then 1
         |                    when conversion_goal = 2 and b.cv_types like '%cvr2%' then 1
         |                    when conversion_goal = 3 and b.cv_types like '%cvr3%' then 1
         |                    when conversion_goal = 4 and b.cv_types like '%cvr4%' then 1
         |                    when conversion_goal = 0 and is_api_callback = 1 and b.cv_types like '%cvr2%' then 1
         |                    when conversion_goal = 0 and is_api_callback = 0 and (adclass like '11011%' or adclass like '125%') and b.cv_types like '%cvr4%' then 1
         |                    when conversion_goal = 0 and is_api_callback = 0 and adclass not like '11011%' and adclass not like '125%' and b.cv_types like '%cvr%' then 1
         |                    else 0 end as isbuy
         |            from
         |                (
         |                    select
         |                        searchid,
         |                        case when media_appsid in ('80000001', '80000002') then 'Qtt'
         |                             when media_appsid in ('80001098', '80001292', '80001539', '80002480', '80001011', '80004786', '80004787') then 'MiDu'
         |                             when media_appsid in ('80002819', '80004944') then 'HT66'
         |                        else 'other' end as media_tag,
         |                        userid, unitid, hour, cvr_model_name,
         |                        conversion_goal, is_api_callback, adclass,
         |                        ideaid, isshow, isclick,
         |                        bsrawcvr as bs_cvr
         |                        -- if(bscvr / 1000000.0 > 1.0, 1.0, bscvr / 1000000.0) as bs_cvr
         |                    from dl_cpc.cpc_basedata_union_events
         |                    where day = '$date'
         |                        and media_appsid in ('80000001', '80000002')
         |                        and charge_type = 1
         |                        and adsrc = 1
         |                        and isshow > 0
         |                        and isclick > 0
         |                        and array_contains(exptags, 'bslrcvr=bs-v4-cvr')
         |                        and array_contains(exptags, 'bscvrfactor=0')
         |                        and array_contains(exptags, 'bslradtypecorrection')
         |                ) A
         |            left outer join
         |                (
         |                    select
         |                        searchid, concat_ws(',', collect_set(cvr_goal)) as cv_types
         |                    from
         |                        dl_cpc.ocpc_label_cvr_hourly
         |                    where
         |                        `date`>='$date' and `date`<='$tomorrow'
         |                        and label=1
         |                    group by searchid
         |                ) B
         |            on A.searchid=B.searchid
         |        ) AA
         |        distribute by AA.conversion_goal sort by AA.bs_cvr DESC
         |    ) AAA
         |group by AAA.conversion_goal
         |order by AAA.conversion_goal
      """.stripMargin
    println("sql="+bs_sql)
    val bs_df=spark.sql(bs_sql)
    println("============= bs_df ===============")
    bs_df.show(5)

  }
}