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

    val sql =
      s"""
         |select
         |cvr_model_name,
         |auc(raw_cvr, coalesce(label,0)) as auc,
         |avg(raw_cvr)*sum(isclick)/sum(label) as pcoc
         |from
         |(
         |select cvr_model_name, isshow, isclick, raw_cvr/1000000 as raw_cvr, exptags,case when cv_types = null then 0
         |           when conversion_goal = 1 and B.cv_types like '%cvr1%' then 1
         |           when conversion_goal = 2 and B.cv_types like '%cvr2%' then 1
         |           when conversion_goal = 3 and B.cv_types like '%cvr3%' then 1
         |           when conversion_goal = 4 and B.cv_types like '%cvr4%' then 1
         |           when conversion_goal = 0 and is_api_callback = 1 and B.cv_types like '%cvr2%' then 1
         |           when conversion_goal = 0 and is_api_callback = 0 and (adclass like '11011%' or adclass like '125%') and B.cv_types like '%cvr4%' then 1
         |           when conversion_goal = 0 and is_api_callback = 0 and adclass not like '11011%' and adclass not like '125%' and B.cv_types like '%cvr%' then 1
         |      else 0 end as label from
         |  (select
         |  searchid,conversion_goal,is_api_callback,adclass,if(array_contains(exptags, 'bslrcvr=bs-v4-cvr') and array_contains(exptags, 'bscvrfactor=0'), 1, 0) as cvr_model_name, isshow, isclick, raw_cvr*1.0/1000000 as raw_cvr, bsrawcvr*1.0/1000000 as bsrawcvr, exptags
         |  from
         |  dl_cpc.cpc_basedata_union_events
         |  where
         |  day in ('$date')
         |  and isshow=0
         |  and isclick=0
         |  and media_appsid in ('80000001','80000002')
         |  and adsrc=1
         |  and charge_type = 1
         |  and array_contains(exptags, 'bslradtypecorrection'))A
         |  left outer join
         |   (
         |      select
         |      searchid, concat_ws(',', collect_set(cvr_goal)) as cv_types
         |      from
         |         dl_cpc.ocpc_label_cvr_hourly
         |      where
         |         `date`>="$date" and `date`<="$tomorrow"
         |      and label=1
         |      group by searchid
         |   ) B
         |   on A.searchid=B.searchid
         |  distribute by cvr_model_name sort by raw_cvr DESC
         |) t1
         |group by cvr_model_name
      """.stripMargin
    println("sql="+sql)
    val df=spark.sql(sql)
    df.show(5)

  }
}