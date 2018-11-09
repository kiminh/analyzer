package com.cpc.spark.ocpc

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

/**
  * 获取 union_log 中 ocpc类广告
  * author: huazhenhao
  * date: 11/1/18
  */
object GetOcpcLogFromUnionLog {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString
    val timeRange = s"`date`='$date' and hour = '$hour'"
    val sqlRequest =
      s"""
        | select
        |  a.searchid,
        |  a.timestamp,
        |  a.uid,
        |  a.ext['exp_ctr'].int_value * 1.0 / 1000000 as exp_ctr,
        |  a.ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
        |  a.ideaid,
        |  a.price,
        |  a.userid,
        |  a.ext['adclass'].int_value as adclass,
        |  a.isclick,
        |  a.isshow,
        |  a.exptags,
        |  a.ext_int['bid_ocpc'] as cpa_given,
        |  a.ext_string['ocpc_log'] as ocpc_log,
        |  b.label2 as iscvr,
        |  '$date' as dt,
        |  '$hour' as hour
        | from
        |      (
        |        select *
        |        from dl_cpc.cpc_union_log
        |        where $timeRange
        |        and media_appsid  in ("80000001", "80000002")
        |        and round(ext["adclass"].int_value/1000) != 132101  --去掉互动导流
        |        and isshow = 1
        |        and ext['antispam'].int_value = 0
        |        and ideaid > 0
        |        and adsrc = 1
        |        and adslot_type in (1,2,3)
        |        and ext_int['is_ocpc'] = 1
        |      ) a
        |left join
        |      (
        |        select searchid, label2
        |        from dl_cpc.ml_cvr_feature_v1
        |        where $timeRange
        |      ) b on a.searchid = b.searchid
      """.stripMargin

    println(sqlRequest)

    var df = spark.sql(sqlRequest)
    df = df.withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
    // switch the last column with the last but three
    val cols = df.columns
    val ocpc_log_dict = cols(cols.length - 1)
    cols(cols.length - 1) = cols(cols.length - 2)
    cols(cols.length - 2) = cols(cols.length - 3)
    cols(cols.length - 3) = ocpc_log_dict

    df = df.select(cols.head, cols.tail: _*)

    println(s"output size: ${df.count()}")
    println("first 10 rows: ")
    df.show(10)
    df.write.mode("append").insertInto("dl_cpc.ocpc_unionlog")
  }
}