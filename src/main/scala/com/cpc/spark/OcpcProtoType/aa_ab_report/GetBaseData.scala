package com.cpc.spark.OcpcProtoType.aa_ab_report

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object GetBaseData {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val hour = args(1).toString
    val spark = SparkSession.builder().appName("GetBaseData").enableHiveSupport().getOrCreate()
    getBaseData(date, hour, spark)
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
         |          when cast(adclass as string) like "100%" then 'app'
         |          when adclass in (110110100, 125100100) then 'wzcp'
         |          else 'others' end) as industry,
         |    (case when exptags not like "%,cpcBid%" and exptags not like "%cpcBid,%" then "ocpc"
         |          else "cpc" end) as exptags,
         |    is_ocpc,
         |    isclick,
         |    isshow,
         |    bid,
         |    conversion_goal,
         |    price,
         |    exp_cvr,
         |    ocpc_log
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
         |    (charge_type = 1 or charge_type is null)
         |and
         |    `date` = '$date'
      """.stripMargin
    // 当hour等于-1时，表示取全天的数据
    if("all".equals(hour) == false) sql1 +=  s" and hour = '$hour'"
    println("------sql1--------")
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
         |    exptags,
         |    is_ocpc,
         |    isclick,
         |    isshow,
         |    (case when length(ocpc_log) > 0 then cast(ocpc_log_dict['dynamicbid'] as int)
         |          else bid end) as bid,
         |    (case when is_ocpc = 1 then cast(ocpc_log_dict['conversiongoal'] as int)
         |          else conversion_goal end) as conversion_goal,
         |    (case when is_ocpc = 1 then cast(ocpc_log_dict['pcvr'] as double)
         |          else 0 end) as pcvr,
         |    (case when is_ocpc = 1 then cast(ocpc_log_dict['IsHiddenOcpc'] as int)
         |          else 0 end) as is_hidden,
         |    (case when is_ocpc = 1 then cast(ocpc_log_dict['kvalue'] as double)
         |          else 0 end) as kvalue,
         |    (case when is_ocpc = 1 then cast(ocpc_log_dict['budget'] as int)
         |          else 0 end) as budget,
         |    (case when is_ocpc = 1 then cast(ocpc_log_dict['cpagiven'] as double)
         |          else 0 end) as cap_given,
         |    price,
         |    exp_cvr
         |from
         |    temp_table1
      """.stripMargin
    spark.sql(sql2).createOrReplaceTempView("temp_table2")
    println("------sql2--------")
    println(sql2)

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
    println("------sql3--------")
    println(sql3)
    val baseDataDF = spark.sql(sql3)
    baseDataDF
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .withColumn("version", lit("qtt_demo"))
      .repartition(400)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_aa_ab_report_base_data")
  }

}
