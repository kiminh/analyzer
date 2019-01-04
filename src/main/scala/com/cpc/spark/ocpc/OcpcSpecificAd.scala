package com.cpc.spark.ocpc

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object OcpcSpecificAd {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val dt = args(0)
    val hour = args(1)
    var selectWhere = s"`date`='$dt' and hour = '$hour'"
    var sqlRequest =
      s"""
         | select
         |  a.searchid,
         |  a.uid,
         |  a.price,
         |  a.userid,
         |  a.ideaid,
         |  a.unitid,
         |  a.planid,
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
         |inner join
         |      (
         |        select searchid, label
         |        from dl_cpc.ml_cvr_feature_v1
         |        where $selectWhere and label_type!=12
         |      ) b on a.searchid = b.searchid
      """.stripMargin
    println(sqlRequest)
    val base = spark.sql(sqlRequest)

    val columns = base.columns
    columns.foreach(println)

    base.createOrReplaceTempView("tmpTable")
    val groupByRequesst =
      s"""
         |SELECT
         |  uid,
         |  userid,
         |  ideaid,
         |  unitid,
         |  planid
         |FROM
         |  tmpTable
         |GROUP BY uid, userid, ideaid, unitid, planid
       """.stripMargin

    val groupBy = spark.sql(groupByRequesst)
    val result = groupBy.withColumn("date", lit(dt))
      .withColumn("hour", lit(hour))

    // save data
    result.write.mode("overwrite").insertInto("test.check_union_log")

    println("successfully save data into table test.check_union_log")
  }
}
