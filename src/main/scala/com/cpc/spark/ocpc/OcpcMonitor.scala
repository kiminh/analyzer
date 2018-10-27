package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import com.cpc.spark.udfs.Udfs_wj._
import org.apache.spark.sql.functions._

object OcpcMonitor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("OcpcMonitor").enableHiveSupport().getOrCreate()

    val day = args(0).toString
    val hour = args(1).toString

    val sqlRequest =
      s"""
         |SELECT
         |    a.uid,
         |    a.timestamp,
         |    a.searchid,
         |    a.exp_ctr,
         |    a.exp_cvr,
         |    a.isclick,
         |    a.isshow,
         |    a.ideaid,
         |    a.adclass,
         |    a.exptags,
         |    a.price,
         |    a.bid_ocpc,
         |    a.ocpc_log,
         |    b.iscvr,
         |    '$day' as date,
         |    '$hour' as hour
         |FROM
         |    (select
         |        uid,
         |        timestamp,
         |        searchid,
         |        userid,
         |        ext['exp_ctr'].int_value * 1.0 / 1000000 as exp_ctr,
         |        ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |        isclick,
         |        isshow,
         |        ideaid,
         |        ext['adclass'].int_value as adclass,
         |        exptags,
         |        price,
         |        ext_int['bid_ocpc'] as bid_ocpc,
         |        ext_int['is_ocpc'] as is_ocpc,
         |        ext_string['ocpc_log'] as ocpc_log
         |    from
         |        dl_cpc.cpc_union_log
         |    WHERE
         |        `date` = '$day'
         |    and
         |        `hour` = '$hour'
         |    and
         |        media_appsid  in ("80000001", "80000002")
         |    and
         |        ext['antispam'].int_value = 0
         |    and adsrc = 1
         |    and adslot_type in (1,2,3)
         |    and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
         |    AND ext_int['is_ocpc']=1
         |    AND ext_string['ocpc_log'] != ''
         |    AND ext_string['ocpc_log'] is not null) a
         |left outer join
         |    (
         |        select
         |            searchid,
         |            label2 as iscvr
         |        from dl_cpc.ml_cvr_feature_v1
         |        WHERE
         |            `date` = '$day'
         |        and
         |            `hour` = '$hour'
         |    ) b on a.searchid = b.searchid
       """.stripMargin

    println(sqlRequest)

    val dataDF = spark.sql(sqlRequest)

    dataDF.show(10)

    dataDF.write.mode("append").insertInto("dl_cpc.ocpc_result_unionlog_table_bak")

  }
}
