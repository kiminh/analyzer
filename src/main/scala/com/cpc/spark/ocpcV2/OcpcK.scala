package com.cpc.spark.ocpcV2

import org.apache.spark.sql.SparkSession

object OcpcK {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("ocpc v2").enableHiveSupport().getOrCreate()

    // val date = args(0).toString
    // val hour = args(1).toString
    // val onDuty = args(2).toInt

    val dtCondition = "`date` = '2018-11-07' and hour in ('09','10','11')"
    val dtCondition2 = "`dt` = '2018-11-07' and hour in ('09','10','11')"

    val statSql =
      s"""
        |select
        |  ideaid,
        |  ocpc_log_dict['kvalue'] k,
        |  ocpc_log_dict['cpagiven'] cpagiven,
        |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0)) as cpa2,
        |  sum(if(isclick=1,price,0))/sum(COALESCE(label3,0)) as cpa3,
        |  sum(if(isclick=1,price,0))/sum(COALESCE(label2,0))/ocpc_log_dict['cpagiven'] as ratio2,
        |  sum(if(isclick=1,price,0))/sum(COALESCE(label3,0))/ocpc_log_dict['cpagiven'] as ratio3
        |from
        |  (select * from dl_cpc.ocpc_unionlog where $dtCondition2 and ocpc_log_dict['kvalue'] !=null and isclick=1) a
        |  left outer join
        |  (select searchid, label2 from dl_cpc.ml_cvr_feature_v1 where $dtCondition) b on a.searchid = b.searchid
        |  left outer join
        |  (select searhcid, iscvr as label3 from dl_cpc.cpc_api_union_log where $dtCondition) c on a.searchid = c.searchid
        |group by ideaid, ocpc_log_dict['kvalue'], ocpc_log_dict['cpagiven']
      """.stripMargin

      println(statSql)

      spark.sql(statSql).write.mode("overwrite").saveAsTable("test.djq_ocpc")

  }


}
