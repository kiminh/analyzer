package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OcpcSampleHourlyV1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // extract data from hive
    val dt = args(0)
    val hour = args(1)
    var selectWhere = s"`date`='$dt' and hour = '$hour'"

    val siteFormData = spark
      .table("dl_cpc.site_form_unionlog")
      .where(s"`date`='$dt' and `hour`='$hour' and ideaid!=0")
      .select("ideaid", "searchid")
      .withColumn("label", lit(1))
      .distinct()
    siteFormData.createOrReplaceTempView("site_form_data")

    siteFormData
      .select("ideaid")
      .withColumn("flag", lit(1))
      .distinct()
      .createOrReplaceTempView("site_form_ideas")

    var sqlRequest1 =
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
         |  b.flag
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
         |      site_form_ideas b
         |on a.ideaid=b.ideaid
      """.stripMargin
    println(sqlRequest1)
    val unionlog = spark.sql(sqlRequest1)
    unionlog.createOrReplaceTempView("unionlog_table")

    val sqlRequest2 =
      s"""
         |select
         |  a.searchid,
         |  a.uid,
         |  a.ideaid,
         |  a.price,
         |  a.userid,
         |  a.adclass,
         |  a.isclick,
         |  a.isshow,
         |  a.flag,
         |  b.label2 as iscvr1,
         |  c.label as iscvr2
         |from
         |  unionlog_table as a
         |left join
         |  (select searchid, label2 from dl_cpc.ml_cvr_feature_v1 where $selectWhere and label_type!=12) as b
         |on
         |  a.searchid=b.searchid
         |left join
         |  (select searchid, 1 as label from dl_cpc.site_form_unionlog where $selectWhere) as c
         |on
         |  a.searchid=c.searchid
       """.stripMargin
    println(sqlRequest2)
    val base = spark
      .sql(sqlRequest2)
      .withColumn("iscvr", when(col("flag")===1, col("iscvr2")).otherwise(col("iscvr1")))

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
    result
      .repartition(10).write.mode("overwrite").insertInto("dl_cpc.ocpc_uid_userid_track_label2")

    result.show(10)



    println("successfully save data into table dl_cpc.ocpc_uid_userid_track_label2")
  }
}
