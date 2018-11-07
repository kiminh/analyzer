package com.cpc.spark.ocpcV2

import org.apache.spark.sql.SparkSession

object OcpcK {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ocpc v2").enableHiveSupport().getOrCreate()

    // val date = args(0).toString
    // val hour = args(1).toString
    // val onDuty = args(2).toInt

    val dtCondition = "`date` = '2018-11-05' and hour in ('15','16')"
    val dtCondition2 = "`dt` = '2018-11-05' and hour in ('15','16')"

    val statSql =
      s"""
        |select
        |  ideaid,
        |  round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cali'] * 100.0) as k,
        |  ocpc_log_dict['cpagiven'] as cpagiven,
        |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0)) as cpa2,
        |  sum(if(isclick=1,price,0))/sum(COALESCE(label3,0)) as cpa3,
        |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0))/ocpc_log_dict['cpagiven'] as ratio2,
        |  sum(if(isclick=1,price,0))/sum(COALESCE(label3,0))/ocpc_log_dict['cpagiven'] as ratio3,
        |  sum(isclick) clickCnt,
        |  sum(COALESCE(label2,0)) cvr2Cnt,
        |  sum(COALESCE(label3,0)) cvr3Cnt
        |from
        |  (select * from dl_cpc.ocpc_unionlog where $dtCondition2 and ocpc_log_dict['kvalue'] is not null and isclick=1) a
        |  left outer join
        |  (select searchid, label2 from dl_cpc.ml_cvr_feature_v1 where $dtCondition) b on a.searchid = b.searchid
        |  left outer join
        |  (select searchid, iscvr as label3 from dl_cpc.cpc_api_union_log where $dtCondition) c on a.searchid = c.searchid
        |group by ideaid, round(ocpc_log_dict['kvalue'] * ocpc_log_dict['cali'] * 100.0) , ocpc_log_dict['cpagiven']
      """.stripMargin

      println(statSql)

      spark.sql(statSql).write.mode("overwrite").saveAsTable("test.djq_ocpc")

  }


}
