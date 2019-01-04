

package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object OcpcSampleHourly {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // extract data from hive
    val dt = args(0)
    val hour = args(1)
    var selectWhere = s"`date`='$dt' and hour = '$hour'"
    var sqlRequest =
      s"""
         | select
         |  a.searchid,
         |  a.uid,
         |  a.ideaid,
         |  a.price,
         |  a.userid,
         |  a.ext['adclass'].int_value as adclass,
         |  a.isclick,
         |  a.isshow,
         |  b.label as iscvr
         | from
         |      (
         |        select *
         |        from dl_cpc.cpc_union_log
         |        where $selectWhere
         |        and isclick is not null
         |        and media_appsid  in ("80000001", "80000002")
         |        and isshow = 1
         |        and ext['antispam'].int_value = 0
         |        and ideaid > 0
         |        and adsrc = 1
         |        and adslot_type in (1,2,3)
         |      ) a
         |left join
         |      (
         |        select searchid, label
         |        from dl_cpc.ml_cvr_feature_v1
         |        where $selectWhere and label_type!=12
         |      ) b on a.searchid = b.searchid
      """.stripMargin
    println(sqlRequest)
    val base = spark.sql(sqlRequest)

    // recalculation with groupby of userid and uid
    base.createOrReplaceTempView("tmpTable")
    val groupByRequesst =
      s"""
         |Select
         |  userid,
         |  uid,
         |  ideaid,
         |  adclass,
         |  SUM(CASE WHEN isclick == 1 then price else 0 end) as cost,
         |  SUM(CASE WHEN isclick == 1 then 1 else 0 end) as ctr_cnt,
         |  SUM(CASE WHEN iscvr == 1 then 1 else 0 end) as cvr_cnt,
         |  SUM(CASE WHEN isshow == 1 then 1 else 0 end) as total_cnt
         |FROM
         |  tmpTable
         |GROUP BY userid, uid, ideaid, adclass
       """.stripMargin

    val groupBy = spark.sql(groupByRequesst)
    val result = groupBy.withColumn("date", lit(dt))
      .withColumn("hour", lit(hour))

    // save data
    result.write.mode("overwrite").insertInto("dl_cpc.ocpc_uid_userid_track")



    println("successfully save data into table dl_cpc.ocpc_uid_userid_track")
  }
}

