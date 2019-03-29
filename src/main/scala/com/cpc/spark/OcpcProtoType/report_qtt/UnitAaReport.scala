package com.cpc.spark.OcpcProtoType.report_qtt

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

object UnitAaReport {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("UnitAaReport").enableHiveSupport().getOrCreate()
    val df = getBaseData("2019-03-27", "-1", spark)
    df.createOrReplaceTempView("temp_table")
    val sql = "select unitid, (case when length(ocpc_log) > 0 then ocpc_log_dict['cpagiven'] else -1 end) from temp_table limit 10"
    val dataFrame = spark.sql(sql).collectAsList()
    println(dataFrame(0))
  }

  // 首先从base表里获取基础字段
  def getBaseData(date: String, hour: String, spark: SparkSession): DataFrame ={
    var sql =
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
        |    price,
        |    exp_cvr,
        |    ocpc_log,
        |    `date`,
        |    hour
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
    if("-1".equals(hour.toString) == false) sql +=  s" and hour = '$hour'"
    val dataDF = spark.sql(sql)
    dataDF.withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))
  }

  // 获取suggest_cpa
  def getSuggestCpa(date: String, hour: String, spark: SparkSession): DataFrame ={
     var subQuery =
       s"""
         |select
         |    unitid,
         |    userid,
         |    cpa,
         |    row_number() over(partition by unitid, userid order by show desc) as row_num
         |from
         |    dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |where
         |   version = 'qtt_demo'
         |and
         |   `date` = '$date'
       """.stripMargin

    // 判断当前是否是查询全天的数据
    if("-1".equals(hour.toString) == false) subQuery +=  s" and hour = '$hour'"

    val sql =
      s"""
        |select
        |    a.unitid,
        |    a.userid,
        |    round(a.cpa * 0.01, 4) as suggest_cpa
        |from
        |    ($subQuery) a
        |where
        |    a.row_num = 1
      """.stripMargin

    val dataDF = spark.sql(sql)
    dataDF
   }

  // 获取auc
  def getAuc(date: String, hour: String, spark: SparkSession): DataFrame ={
    var sql1 =
      s"""
        |select
        |    a.unitid,
        |    a.userid,
        |    1 as conversion_goal,
        |    a.exp_cvr,
        |    b.iscvr1 as iscvr
        |from
        |    dl_cpc.ocpc_base_unionlog a
        |left join
        |    (select
        |        searchid,
        |        hour,
        |        label2 as iscvr1
        |    from
        |        dl_cpc.ml_cvr_feature_v1
        |    where
        |        `date` = '$date'
        |    and
        |        label2 = 1
        |    and
        |        label_type in (1, 2, 3, 4, 5)
        |    ) as b
        |on
        |    a.searchid = b.searchid
        |where
        |    `date` = '$date'
        |and
        |    a.media_appsid  in ("80000001", "80000002")
        |and
        |    a.isshow = 1
        |and
        |    a.antispam = 0
        |and
        |    a.adslot_type in (1, 2, 3)
        |and
        |    a.adsrc = 1
        |and
        |    (a.charge_type is null or a.charge_type = 1)
      """.stripMargin
    if("-1".equals(hour.toString) == false) sql1 += s" and hour = '$hour'"
    val dataFrame1 = spark.sql(sql1)
    dataFrame1
  }
}
