package com.cpc.spark.OcpcProtoType.aa_ab_report

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class GetBaseData2 {
  def main(args: Array[String]): Unit = {

  }
  def getBaseData(date: String, hour: String, spark: SparkSession): Unit ={
    var sql1 =
      s"""
         |select
         |    searchid,
         |    unitid,
         |    userid,
         |    adslot_type,
         |    (case when (cast(adclass as string) like "134%" or cast(adclass as string) like "107%") then 'elds'
         |      when cast(adclass as string) like "100%" then 'app'
         |      when adclass in (110110100, 125100100) then 'wzcp'
         |      else 'others' end) as industry,
         |    is_ocpc,
         |    isclick,
         |    isshow,
         |    bid,
         |    conversion_goal,
         |    price,
         |    exp_cvr,
         |    ocpc_log,
         |from
         |    dl_cpc.ocpc_base_unionlog
         |where
         |    media_appsid  in ("80000001", "80000002")
         |and
         |    isshow = 1
         |and
         |    antispam = 0
         |and
         |    adsrc = 1
         |and
         |    `date` = '$date'
      """.stripMargin
    // 当hour等于-1时，表示取全天的数据
    if("all".equals(hour) == false) sql1 +=  s" and hour = '$hour'"
    println(sql1)
    spark.sql(sql1)
      .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
      .createOrReplaceTempView("temp_table1")

    val sql2 =
      s"""
        |select
        |    searchid,
        |    unitid,
        |    userid,
        |    adslot_type,
        |    industry,
        |    is_ocpc,
        |    isclick,
        |    isshow,
        |    (case when length(ocpc_log) > 0 then cast(ocpc_log_dict['bid'] as int)
        |          else bid end) as bid,
        |    (case when length(ocpc_log) = 0 then conversion_goal
        |          else cast(ocpc_log_dict['conversiongoal'] as int) end) as conversion_goal,
        |    (case when length(ocpc_log) > 0 then cast(ocpc_log_dict['pcvr'] as double)
        |          else 0 end) as pcvr,
        |    (case when length(ocpc_log) > 0 then cast(ocpc_log_dict['IsHiddenOcpc'] as int)
        |          else 0 end) as is_hidden,
        |    (case when length(ocpc_log) > 0 then cast(ocpc_log_dict['kvalue'] as int)
        |          else 0 end) as kvalue,
        |    (case when length(ocpc_log) > 0 then cast(ocpc_log_dict['budget'] as int)
        |          else 0 end) as budget,
        |    price,
        |    exp_cvr
        |from
        |    temp_table1
      """.stripMargin
    spark.sql(sql2).createOrReplaceTempView("temp_table2")

    val sql3 =
      s"""
         |select
         |    a.*,
         |    (case when a.conversion_goal = 1 then b.iscvr1
         |          when a.conversion_goal = 2 then c.iscvr2
         |          else d.iscvr3 end) as iscvr
         |from
         |    temp_table2 a
         |left join
         |    (select
         |        searchid,
         |        label2 as iscvr1
         |    from
         |        dl_cpc.ml_cvr_feature_v1
         |    where
         |        `date` = '$date'
         |    and
         |        label2 = 1
         |    and
         |        label_type in (1, 2, 3, 4, 5)
         |    group by
         |        searchid,
         |        label2) as b
         |on
         |    a.searchid = b.searchid
         |left join
         |    (select
         |        searchid,
         |        label as iscvr2
         |    from
         |        dl_cpc.ml_cvr_feature_v2
         |    where
         |        `date` = '$date'
         |    and
         |        label = 1
         |    group by
         |        searchid,
         |        label) as c
         |on
         |    a.searchid = c.searchid
         |left join
         |    (select
         |        searchid,
         |        1 as iscvr3
         |    from
         |        dl_cpc.site_form_unionlog
         |    WHERE
         |        `date` = '$date'
         |    and
         |        ideaid > 0
         |    and
         |        searchid is not null
         |    group by
         |        searchid) as d
         |on
         |    a.searchid = d.searchid
      """.stripMargin
    println("--------------")
    println(sql2)
    val baseDataDF = spark.sql(sql2)
    baseDataDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
      .repartition(50)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_aa_ab_report_base_data")
  }

  // 获得suggest_cpa，只有天级别的
  def getSuggestCpa(date: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
        |select
        |    a.unitid,
        |    a.userid,
        |    round(a.cpa * 0.01, 4) as suggest_cpa
        |from
        |    (select
        |        unitid,
        |        userid,
        |        cpa,
        |        row_number() over(partition by unitid, userid order by cost desc) as row_num
        |    from
        |        dl_cpc.ocpc_suggest_cpa_recommend_hourly
        |    where
        |       version = 'qtt_demo'
        |    and
        |       `date` = '$date'
        |    and
        |        hour = '06') a
        |where
        |    a.row_num = 1
      """.stripMargin
    val suggestCpaDF = spark.sql(sql)
    suggestCpaDF
  }
   // 获取auc，只有天级别的
  def getAuc(date: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
        |select
        |    `date`,
        |    unitid,
        |    auc,
        |    conversion_goal
        |from
        |    dl_cpc.ocpc_unitid_auc_daily
        |where
        |    `date` = '$date'
        |and
        |    version = 'qtt_demo'
      """.stripMargin
    val aucDF = spark.sql(sql)
    aucDF
  }
}
