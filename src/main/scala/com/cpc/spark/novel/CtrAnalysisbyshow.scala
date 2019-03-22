package com.cpc.spark.novel

import org.apache.spark.sql.SparkSession

object CtrAnalysisbyshow {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .getOrCreate()

    val show_sql =
      s"""
         |select uid, ideaid,ctr_model_name,isclick,isshow,exp_ctr,day,adslot_id,adslot_type,user_req_ad_num,
         |      row_number() over(partition by uid,ideaid order by timestamp) rk
         |from dl_cpc.cpc_basedata_union_events
         |where day='2019-03-20'
         |  and isshow = 1
         |  and adsrc = 1
         |  and ctr_model_name = 'qtt-list-dnn-rawid-v4'
         |  and media_appsid in ('80000001','80000002')
         |  AND userid > 0
         |  AND (charge_type IS NULL OR charge_type = 1)
      """.stripMargin

    println(show_sql)
    val qttctr=spark.sql(show_sql)
    qttctr.write.mode("overwrite").saveAsTable("test.qttctrbyshow")

    val sql1=
      s"""
         |select
         |  day,
         |  ctr_model_name,
         |  adslot_id,
         |  rk,
         |  sum(isshow) as imp,
         |  round(sum(exp_ctr)/sum(isshow)/10000,3) as ectr,
         |  round(sum(isclick)*100 / sum(isshow),3) as ctr,
         |  round(sum(exp_ctr)/1000000/sum(isclick),3) as pcoc
         |FROM
         |  test.qttctrbyshow
         |GROUP BY
         |  day,
         |  ctr_model_name,
         |  adslot_id,
         |  rk
         |order by imp desc
       """.stripMargin

    val qttctr2=spark.sql(sql1)
    qttctr2.write.mode("overwrite").saveAsTable("test.qttctrbyshow_id")

    val sql2=
      s"""
         |select uid, ideaid,ctr_model_name,isclick,isshow,exp_ctr,day,adslot_id,adslot_type,user_req_ad_num,
         |      row_number() over(partition by uid,ideaid order by timestamp) rk
         |from dl_cpc.cpc_novel_union_events
         |where day='2019-03-20'
         |  and isshow = 1
         |  and adsrc = 1
         |  and ctr_model_name = 'novel-ctr-dnn-rawid-v7'
         |  and media_appsid in ('80001098','80001292')
         |  AND userid > 0
         |  AND (charge_type IS NULL OR charge_type = 1)
       """.stripMargin

    println(sql2)
    val novelctr=spark.sql(sql2)

    novelctr.write.mode("overwrite").saveAsTable("test.novelctrbyshow")

    val sql3=
      s"""
         |select
         |  day,
         |  ctr_model_name,
         |  adslot_id,
         |  rk,
         |  sum(isshow) as imp,
         |  round(sum(exp_ctr)/sum(isshow)/10000,3) as ectr,
         |  round(sum(isclick)*100 / sum(isshow),3) as ctr,
         |  round(sum(exp_ctr)/1000000/sum(isclick),3) as pcoc
         |FROM
         |  test.novelctrbyshow
         |GROUP BY
         |  day,
         |  ctr_model_name,
         |  adslot_id,
         |  rk
         |order by imp desc
       """.stripMargin

    val qttctr3=spark.sql(sql3)
    qttctr3.write.mode("overwrite").saveAsTable("test.novelctrbyshow_id")


  }

}
