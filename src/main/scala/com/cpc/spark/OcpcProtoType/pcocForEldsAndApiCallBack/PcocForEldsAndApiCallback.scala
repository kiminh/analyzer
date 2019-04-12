package com.cpc.spark.OcpcProtoType.pcocForEldsAndApiCallBack

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

object PcocForEldsAndApiCallback {
  def main(args: Array[String]): Unit = {
    val date = args(0).toString
    val spark = SparkSession.builder().appName("PcocForEldsAndApiCallback").enableHiveSupport().getOrCreate()
    val unitDF = getUnit(date, spark)
    println("------has got api callback unitids-------")
    val baseDataDF = getBaseData(date, spark)
    println("------has got base data-------")
    val valueDF = calculateValue(baseDataDF, spark)
    println("------has calculate pre_cvr and post_cvr-------")
    calculateApiPcoc(unitDF, valueDF, date, spark)
    println("------has got api callback's pcoc-------")
    calculateEldsPcoc(valueDF, date, spark)
    println("------has got eld's pcoc-------")
  }

  // 获取每天api回传的unitid
  def getUnit(date: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
        |select
        |    dt,
        |    unitid
        |from
        |    dl_cpc.slim_union_log
        |where
        |    dt = '$date'
        |and
        |    is_api_callback = 1
        |and
        |    media_appsid  in ("80000001", "80000002")
        |and
        |    isshow = 1
        |and
        |    adslot_type != 7
        |and
        |    antispam = 0
        |and
        |    adsrc = 1
        |group by
        |    dt,
        |    unitid
      """.stripMargin
    val unitDF = spark.sql(sql)
    unitDF
  }

  // 从base表里获取基础数据
  def getBaseData(date: String, spark: SparkSession): DataFrame ={
    val sql =
      s"""
        |select
        |    a.searchid,
        |    a.unitid,
        |    a.isclick,
        |    a.pcvr,
        |    a.conversion_goal,
        |    a.adclass,
        |    (case when a.conversion_goal = 1 then b.iscvr1
        |          when a.conversion_goal = 2 then c.iscvr2
        |          else d.iscvr3 end) as iscvr,
        |    `date`
        |from
        |    (select
        |        searchid,
        |        unitid,
        |        isclick,
        |        exp_cvr as pcvr,
        |        conversion_goal,
        |        adclass,
        |        `date`
        |    from
        |        dl_cpc.ocpc_base_unionlog
        |    where
        |        `date` = '$date'
        |    and
        |        media_appsid  in ("80000001", "80000002")
        |    and
        |        isshow = 1
        |    and
        |        antispam = 0
        |    and
        |        adsrc = 1
        |    ) as a
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
        |    group by searchid, label2) as b
        |on
        |    a.searchid=b.searchid
        |left join
        |    (select
        |        searchid,
        |        label as iscvr2
        |    from
        |        dl_cpc.ml_cvr_feature_v2
        |    where
        |        `date` = '$date'
        |    and
        |        label=1
        |    group by searchid, label) as c
        |on
        |    a.searchid = c.searchid
        |left join
        |    (select
        |        searchid,
        |        1 as iscvr3
        |    from
        |        dl_cpc.site_form_unionlog
        |    where
        |        `date` = '$date'
        |    and
        |        ideaid > 0
        |    and
        |        searchid is not null
        |    group by searchid) as d
        |on
        |    a.searchid = d.searchid
      """.stripMargin
    val baseDataDF = spark.sql(sql)
    baseDataDF
  }

  // 统计pre_cvr和post_cvr
  def calculateValue(baseDataDF: DataFrame, spark: SparkSession): DataFrame ={
    baseDataDF.createOrReplaceTempView("base_data")
    val sql =
      s"""
        |select
        |    `date`,
        |    unitid,
        |    adclass,
        |    round(sum(case when isclick = 1 then pcvr else 0 end) * 1.0 / sum(isclick), 5) as pre_cvr,
        |    round(sum(iscvr) * 1.0 / sum(isclick), 5) as post_cvr,
        |    round(sum(case when isclick = 1 then pcvr else 0 end) * 1.0 / sum(iscvr), 3) as pcoc
        |from
        |    base_data
        |group by
        |    `date`,
        |    unitid,
        |    adclass
      """.stripMargin
    val valueDF = spark.sql(sql)
    valueDF
  }

  // 计算pcoc
  def calculateApiPcoc(unitDF: DataFrame, valueDF: DataFrame, date: String, spark: SparkSession): Unit ={
    unitDF.createOrReplaceTempView("api_callback_unitid")
    valueDF.createOrReplaceTempView("pcoc_base_value")
    val sql =
      s"""
        |select
        |    a.unitid,
        |    b.pre_cvr,
        |    b.post_cvr,
        |    b.pcoc
        |from
        |    api_callback_unitid a
        |left join
        |    pcoc_base_value b
        |on
        |    a.dt = b.`date`
        |and
        |    a.unitid = b.unitid
      """.stripMargin
    val data = spark.sql(sql)
                    .withColumn("date", lit(date))
                    .withColumn("type", lit("api_callback"))
    // 存hive表
    data
      .repartition(10)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_elds_api_callback_pcoc")
    println("----- has insert into hive -----")
    // 存hadoop
    val path = s"/user/cpc/wentao/pcoc_report/api_callback/$date"
    WriteCsv.writeCsv(data, path, spark)
    println("----- has insert into hadoop -----")
  }

  // 获得二类电商的pcoc
  def calculateEldsPcoc(valueDF: DataFrame, date: String, spark: SparkSession): Unit ={
    valueDF.createOrReplaceTempView("pcoc_base_value")
    val sql =
      s"""
        |select
        |    unitid,
        |    pre_cvr,
        |    post_cvr,
        |    pcoc
        |from
        |    pcoc_base_value
        |where
        |    `date` = '$date'
        |and
        |    (cast(adclass as string) like "134%" or cast(adclass as string) like "107%")
      """.stripMargin
    val data = spark.sql(sql)
                    .withColumn("date", lit(date))
                    .withColumn("type", lit("elds"))
    // 存hive表
    data
      .repartition(10)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_elds_api_callback_pcoc")
    println("----- has insert into hive -----")
    // 存hadoop
    val path = s"/user/cpc/wentao/pcoc_report/elds/$date"
    WriteCsv.writeCsv(data, path, spark)
    println("----- has insert into hadoop -----")
  }

}
