package com.cpc.spark.ml.cvrmodel.daily

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

    val sql =
      s"""
         |create temporary function auc as 'hivemall.evaluation.AUCUDAF' using jar "/home/cpc/anal/lib/hivemall-all-0.5.2-incubating.jar";
         |select
         |ctr_model_name,
         |count(*) as show_cnt,
         |auc(A.exp_ctr/1000000, coalesce(A.isclick,0)) as auc,
         |sum(A.exp_ctr)/1000000/sum(A.isclick) as pcoc
         |from
         |(
         |  select
         |  if(array_contains(exptags, 'bslrcvr=bs-v4-cvr') and array_contains(exptags, 'bscvrfactor=0'), 'bs-v4-cvr', 'no-cvr') as ctr_model_name, isshow, isclick, exp_ctr, exptags
         |  from
         |  dl_cpc.cpc_basedata_union_events
         |  where
         |  day in ('$date')
         |  and media_appsid in ('80000001','80000002')
         |  and array_contains(exptags, 'bslradtypecorrection')
         |  distribute by if(array_contains(exptags, 'bslrcvr=bs-v4-cvr') and array_contains(exptags, 'bscvrfactor=0'), 'bs-v4-cvr', 'no-cvr') sort by exp_ctr DESC
         |) A
         |group by ctr_model_name;
      """.stripMargin
    val df=spark.sql(sql)
    df.show(5)

  }
}