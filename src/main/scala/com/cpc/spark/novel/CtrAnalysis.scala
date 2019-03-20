package com.cpc.spark.novel

import org.apache.spark.sql.SparkSession

object CtrAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val sql=
      s"""
         |select
         |  day,
         |  ctr_model_name,
         |  ideaid,
         |  adslot_id,
         |  sum(isshow) as imp,
         |  round(sum(exp_ctr)/sum(isshow)/10000,3) as ectr,
         |  round(sum(isclick)*100 / sum(isshow),3) as ctr,
         |  round(sum(exp_ctr)/1000000/sum(isclick),3) as pcoc
         |FROM
         |  dl_cpc.cpc_basedata_union_events
         |WHERE day = '2019-03-19'
         |  and isshow = 1
         |  and adsrc = 1
         |  and ctr_model_name = 'qtt-list-dnn-rawid-v4'
         |  and media_appsid in ('80000001','80000002')
         |  AND userid > 0
         |  AND (charge_type IS NULL OR charge_type = 1)
         |GROUP BY
         |  day,
         |  ctr_model_name,
         |  ideaid,
         |  adslot_id
         |order by imp desc
       """.stripMargin

    println(sql)
    val qttctr=spark.sql(sql).filter("imp>1000")

    qttctr.write.mode("overwrite").saveAsTable("test.qttctr")

    val sql2=
      s"""
         |select
         |  day,
         |  ctr_model_name,
         |  ideaid,
         |  adslot_id,
         |  sum(isshow) as imp,
         |  round(sum(exp_ctr)/sum(isshow)/10000,3) as ectr,
         |  round(sum(isclick)*100 / sum(isshow),3) as ctr,
         |  round(sum(exp_ctr)/1000000/sum(isclick),3) as pcoc
         |FROM
         |  dl_cpc.cpc_novel_union_events
         |WHERE day = '2019-03-19'
         |  and isshow = 1
         |  and adsrc = 1
         |  and ctr_model_name = 'novel-ctr-dnn-rawid-v7'
         |  and media_appsid in ('80001098','80001292')
         |  AND userid > 0
         |  AND (charge_type IS NULL OR charge_type = 1)
         |GROUP BY
         |  day,
         |  ctr_model_name,
         |  ideaid,
         |  adslot_id
         |order by imp desc
       """.stripMargin

    println(sql2)
    val novelctr=spark.sql(sql2).filter("imp>1000")

    novelctr.write.mode("overwrite").saveAsTable("test.novelctr")


  }

}
