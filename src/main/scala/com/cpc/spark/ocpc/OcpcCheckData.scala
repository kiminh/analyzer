package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcCheckData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val date = args(0).toString
    val hour = args(1).toString

    val sqlRequest =
      s"""
         |SELECT
         |    b.*,
         |    c.iscvr
         |FROM
         |    (SELECT
         |        t.userid,
         |        t.cost
         |    FROM
         |        (SELECT
         |            userid,
         |            SUM(case when isclick=1 then price else 0 end) as cost
         |        FROM
         |            test.ocpcv3_complete_data20181220
         |        GROUP BY userid) as t
         |    ORDER BY t.cost DESC
         |    limit 100) as a
         |LEFT JOIN
         |    (select
         |        uid,
         |        timestamp,
         |        searchid,
         |        userid,
         |        unitid,
         |        ext['exp_ctr'].int_value * 1.0 / 1000000 as exp_ctr,
         |        ext['exp_cvr'].int_value * 1.0 / 1000000 as exp_cvr,
         |        isclick,
         |        isshow,
         |        ideaid,
         |        exptags,
         |        media_appsid,
         |        price,
         |        bid,
         |        adslotid,
         |        ext["adclass"].int_value as adclass,
         |        ext_int['bid_ocpc'] as bid_ocpc,
         |        ext_int['is_ocpc'] as is_ocpc,
         |        ext_string['ocpc_log'] as ocpc_log,
         |        ext['usertype'].int_value as usertype
         |    from
         |        dl_cpc.cpc_union_log
         |    WHERE
         |        `date` = "$date" and `hour`='$hour'
         |    and
         |        media_appsid  in ("80001098","80001292","80000001", "80000002")
         |    and
         |        ext['antispam'].int_value = 0
         |    and adsrc = 1
         |    and adslot_type in (1,2,3)) b
         |ON
         |    a.userid=b.userid
         |LEFT JOIN
         |    (
         |        select
         |            searchid,
         |            label2 as iscvr
         |        from dl_cpc.ml_cvr_feature_v1
         |        WHERE `date` = "$date" and `hour` = '$hour'
         |    ) c
         | on
         |    b.searchid = c.searchid
       """.stripMargin
    println(sqlRequest)
    val data = spark
      .sql(sqlRequest)
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))

//    data.write.mode("overwrite").saveAsTable("test.test_ocpc_complete_probe_20181208_new")
    data.write.mode("overwrite").insertInto("test.test_ocpc_complete_probe_20181208_new_v1")
  }

}
