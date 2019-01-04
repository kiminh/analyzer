package com.cpc.spark.ocpcV3.ocpcNovel.report

import java.util.Properties
import com.typesafe.config.ConfigFactory

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object OcpcDetailReport {
  def main(args: Array[String]): Unit = {
    /*
    小说媒体上的ocpc广告的详情数据，主要用来比对每个广告单元的ocpc cpa和 cpc cpa
     */
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()

    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString

    // 获取数据
    getOcpcRaw(date, hour, spark)
    val ocpcData = getOcpcData(date, hour, spark)
    ocpcData.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_ocpc_unitid")
    getCpcRaw(date, hour, spark)
    val cpcData = getCPCdata(date, hour, spark)
    cpcData.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_cpc_unitid")

    val cmpModel = cmpByModel(date, hour, spark)
    val tableName1 = "dl_cpc.ocpcv3_novel_cmp_model_hourly"
    cmpModel
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .write
      .mode("overwrite")
      .insertInto(tableName1)
    println(s"successfully save data into table: $tableName1")

    val cmpUnitid = cmpByUnitid(date, hour, spark)
    val tableName2 = "dl_cpc.ocpcv3_novel_cmp_unitid_hourly"
    cmpUnitid
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .write
      .mode("overwrite")
      .insertInto(tableName2)
    println(s"successfully save data into table: $tableName2")

    val result = getCmpDetail(tableName2, date, hour, spark)
    val tableName3 = "dl_cpc.ocpcv3_novel_cmp_detail_hourly"
    result
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hour))
      .write
      .mode("overwrite")
      .insertInto(tableName3)
    println(s"successfully save data into table: $tableName3")

  }

  def getOcpcRaw(date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date`='$date' and `hour` <= '$hour'"

    val sqlRequest =
      s"""
         |SELECT
         |    a.*,
         |    b.iscvr
         |FROM
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
         |        price,
         |        ext_int['bid_ocpc'] as bid_ocpc,
         |        ext_int['is_ocpc'] as is_ocpc,
         |        ext_string['ocpc_log'] as ocpc_log,
         |        hour
         |    from
         |        dl_cpc.cpc_novel_union_log
         |    WHERE
         |        $selectCondition
         |    and
         |        ext['antispam'].int_value = 0
         |    and adsrc = 1
         |    and adslot_type in (1,2,3)
         |    and round(ext["adclass"].int_value/1000) != 132101
         |    and ext_string['ocpc_log'] is not null
         |    and ext_string['ocpc_log'] != '') a
         |left outer join
         |    (
         |        select
         |            searchid,
         |            label2 as iscvr
         |        from dl_cpc.ml_cvr_feature_v1
         |        WHERE $selectCondition and label_type!=12
         |    ) b on a.searchid = b.searchid
       """.stripMargin

    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_ocpc_raw")
    println(s"successfully save data into table: test.ocpcv3_novel_ocpc_raw")
  }

  def getOcpcData(date: String, hour: String, spark: SparkSession) = {
    /*
    小说媒体上ocpc广告的汇总统计信息
     */
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    SUM(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpa,
         |    SUM(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |    sum(isshow) as show_cnt,
         |    sum(isclick) as ctr_cnt,
         |    sum(iscvr) as cvr1_cnt,
         |    sum(iscvr) * 1.0 / sum(isclick) as cvr1,
         |    sum(case when isclick=1 then price else 0 end) as cost
         |FROM
         |    test.ocpcv3_novel_ocpc_raw
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF.show(10)
    resultDF
  }

  def getCpcRaw(date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date`='$date' and `hour` <= '$hour'"
//    data.createOrReplaceTempView("ocpc_data")
    val sqlRequest =
      s"""
         |SELECT
         |    b.*,
         |    c.iscvr
         |FROM
         |    test.ocpcv3_novel_ocpc_unitid as a
         |INNER JOIN
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
         |        price,
         |        ext_int['bid_ocpc'] as bid_ocpc,
         |        ext_int['is_ocpc'] as is_ocpc,
         |        ext_string['ocpc_log'] as ocpc_log,
         |        hour
         |    from
         |        dl_cpc.cpc_novel_union_log
         |    WHERE
         |        $selectCondition
         |    and
         |        ext['antispam'].int_value = 0
         |    and adsrc = 1
         |    and adslot_type in (1,2,3)
         |    and round(ext["adclass"].int_value/1000) != 132101
         |    and (ext_string['ocpc_log'] is null or ext_string['ocpc_log'] = '')) as b
         |ON
         |    a.unitid=b.unitid
         |left outer join
         |    (
         |        select
         |            searchid,
         |            label2 as iscvr
         |        from dl_cpc.ml_cvr_feature_v1
         |        WHERE $selectCondition and label_type!=12
         |    ) as c
         |on  b.searchid = c.searchid
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF.write.mode("overwrite").saveAsTable("test.ocpcv3_novel_cpc_raw")
    println(s"successfully save data into table: test.ocpcv3_novel_cpc_raw")
  }

  def getCPCdata(date: String, hour: String, spark: SparkSession) = {
    /*
    小说媒体上同时在跑ocpc和cpc广告的cpc数据
     */
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    SUM(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpa,
         |    SUM(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |    sum(isshow) as show_cnt,
         |    sum(isclick) as ctr_cnt,
         |    sum(iscvr) as cvr1_cnt,
         |    sum(iscvr) * 1.0 / sum(isclick) as cvr1,
         |    sum(case when isclick=1 then price else 0 end) as cost
         |FROM
         |    test.ocpcv3_novel_cpc_raw
         |GROUP BY unitid
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF.show(10)
    resultDF
  }

  def cmpByModel(date: String, hour: String, spark: SparkSession) = {
    /*
    比较ocpc与cpc的汇总信息：cpm, arpu等
     */
    // ocpc数据
    val sqlRequestOCPC =
      s"""
         |SELECT
         |    'ocpc' as ad_model,
         |    sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpa,
         |    round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
         |    round(sum(case WHEN isclick == 1 then price else 0 end)*10/count(distinct uid),3) as arpu,
         |    sum(isclick) * 1.0 / sum(isshow) as ctr,
         |    sum(iscvr) * 1.0 / sum(isclick) as click_cvr,
         |    sum(iscvr) * 1.0 / sum(isshow) as show_cvr,
         |    sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |    COUNT(isshow) as show_cnt,
         |    SUM(isclick) as ctr_cnt,
         |    SUM(iscvr) as cvr_cnt
         |FROM
         |    test.ocpcv3_novel_ocpc_raw
       """.stripMargin
    println(sqlRequestOCPC)
    val ocpcData = spark.sql(sqlRequestOCPC)

    // cpc数据
    val sqlRequestCPC =
      s"""
         |SELECT
         |    'cpc' as ad_model,
         |    sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpa,
         |    round(sum(case WHEN isclick == 1 then price else 0 end)*10/sum(isshow),3) as cpm,
         |    round(sum(case WHEN isclick == 1 then price else 0 end)*10/count(distinct uid),3) as arpu,
         |    sum(isclick) * 1.0 / sum(isshow) as ctr,
         |    sum(iscvr) * 1.0 / sum(isclick) as click_cvr,
         |    sum(iscvr) * 1.0 / sum(isshow) as show_cvr,
         |    sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |    COUNT(isshow) as show_cnt,
         |    SUM(isclick) as ctr_cnt,
         |    SUM(iscvr) as cvr_cnt
         |FROM
         |    test.ocpcv3_novel_cpc_raw
       """.stripMargin
    println(sqlRequestCPC)
    val cpcData = spark.sql(sqlRequestCPC)

    // 汇总union数据，并存储数据
    val resultDF = ocpcData.union(cpcData)
    resultDF.show(10)
    resultDF
  }

  def cmpByUnitid(date: String, hour: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |    a.unitid,
         |    a.cpa as cpa_ocpc,
         |    b.cpa as cpa_cpc,
         |    a.acp as acp_ocpc,
         |    b.acp as acp_cpc,
         |    a.cvr1 as cvr_ocpc,
         |    b.cvr1 as cvr_cpc,
         |    a.cost as cost_ocpc,
         |    b.cost as cost_cpc
         |FROM
         |    test.ocpcv3_novel_ocpc_unitid as a
         |INNER JOIN
         |    test.ocpcv3_novel_cpc_unitid as b
         |ON
         |    a.unitid=b.unitid
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)
    resultDF.show(10)
    resultDF
  }

  def getCmpDetail(tableName: String, date: String, hour: String, spark: SparkSession) = {
    val selectCondition = s"`date`='$date' and `hour`='$hour'"
    val sqlRequest =
      s"""
         |SELECT
         |    a.unitid,
         |    a.cpa_ocpc,
         |    a.cpa_cpc,
         |    c.cpa_history,
         |    c.adclass_cpa,
         |    a.acp_ocpc,
         |    a.acp_cpc,
         |    a.cost_ocpc,
         |    a.cost_cpc,
         |    (a.cost_ocpc + a.cost_cpc) as cost,
         |    a.cpa_ocpc * 1.0 / a.cpa_cpc as cparatio1,
         |    a.cpa_cpc * 1.0 / c.cpa_history as cparatio2,
         |    a.cost_ocpc * 1.0 / (a.cost_ocpc + a.cost_cpc) as costratio
         |FROM
         |    (SELECT
         |        *
         |    FROM
         |        $tableName
         |    WHERE
         |        $selectCondition) as a
         |LEFT JOIN
         |    (SELECT
         |        unitid,
         |        adclass_cpa,
         |        cpa_history
         |    FROM
         |        dl_cpc.ocpcv3_novel_pb_v2_hourly_middle
         |    WHERE
         |        $selectCondition) as c
         |ON
         |    a.unitid=c.unitid
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF.show(10)
    resultDF
  }
}
