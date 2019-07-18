package com.cpc.spark.OcpcProtoType.report

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.OcpcProtoType.OcpcTools._
import com.cpc.spark.udfs.Udfs_wj._
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcDailyStatReport {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    println("parameters:")
    println(s"date=$date")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcDailyFunnelIndustryV2: $date").enableHiveSupport().getOrCreate()

    val rawData = getOcpcLog(date, spark)

    val data1 = calculateBase(rawData, date, spark)

    val result1 = data1
      .select("unitid", "planid", "userid", "click", "show", "cv", "cost", "ocpc_cpagiven", "ocpc_cpareal", "ocpc_click", "ocpc_show", "ocpc_cv", "ocpc_cost", "hidden_cpagiven", "hidden_cpareal", "hidden_click", "hidden_show", "hidden_cv", "hidden_cost", "budget", "adslot_type", "site_type", "media", "is_ocpc", "industry", "date")

    result1
      .repartition(5)
      .write.mode("overwrite").insertInto("test.ocpc_funnel_data_industry_daily")
//      .write.mode("overwrite").insertInto("dl_cpc.ocpc_funnel_data_industry_daily")


  }

  def calculateCnt(baseData: DataFrame, date: String, spark: SparkSession) = {
    // 计算日期
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`dt` = '$date1'"

    baseData.createOrReplaceTempView("base_data")

    val sqlRequest =
      s"""
         |SELECT
         |    industry,
         |    media,
         |    count(distinct ideaid) as ideaid_cnt,
         |    count(distinct unitid) as unitid_cnt,
         |    count(distinct userid) as userid_cnt
         |FROM
         |    base_data
         |GROUP BY industry, media
       """.stripMargin
    println(sqlRequest)
    val data = spark
        .sql(sqlRequest)
        .withColumn("date", lit(date1))

    data
  }

  def calculateBase(baseData: DataFrame, date: String, spark: SparkSession) = {
    // 汇总数据
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |    unitid,
         |    planid,
         |    userid,
         |    industry,
         |    adslot_type,
         |    media,
         |    sum(isclick) as click,
         |    sum(isshow) as show,
         |    sum(iscvr) as cv,
         |    sum(case when isclick=1 then price else 0 end) * 0.01 as cost,
         |    sum(case when cpc_type = 'ocpc' and isclick=1 then cpagiven else 0 end) * 1.0 / sum(case when cpc_type = 'ocpc' and isclick=1 then 1 else 0 end) as ocpc_cpagiven,
         |    sum(case when cpc_type = 'ocpc' and isclick=1 then price else 0 end) * 1.0 / sum(case when cpc_type = 'ocpc' and iscvr=1 then 1 else 0 end) as ocpc_cpareal,
         |    sum(case when cpc_type = 'ocpc' and isclick=1 then 1 else 0 end) as ocpc_click,
         |    sum(case when cpc_type = 'ocpc' and isshow=1 then 1 else 0 end) as ocpc_show,
         |    sum(case when cpc_type = 'ocpc' and iscvr=1 then 1 else 0 end) as ocpc_cv,
         |    sum(case when cpc_type = 'ocpc' and isclick=1 then price else 0 end) * 0.01 as ocpc_cost,
         |    sum(case when cpc_type = 'hidden_ocpc' and isclick=1 then cpagiven else 0 end) * 1.0 / sum(case when cpc_type = 'hidden_ocpc' and isclick=1 then 1 else 0 end) as hidden_cpagiven,
         |    sum(case when cpc_type = 'hidden_ocpc' and isclick=1 then price else 0 end) * 1.0 / sum(case when cpc_type = 'hidden_ocpc' and iscvr=1 then 1 else 0 end) as hidden_cpareal,
         |    sum(case when cpc_type = 'hidden_ocpc' and isclick=1 then 1 else 0 end) as hidden_click,
         |    sum(case when cpc_type = 'hidden_ocpc' and isshow=1 then 1 else 0 end) as hidden_show,
         |    sum(case when cpc_type = 'hidden_ocpc' and iscvr=1 then 1 else 0 end) as hidden_cv,
         |    sum(case when cpc_type = 'hidden_ocpc' and isclick=1 then price else 0 end) * 0.01 as hidden_cost
         |FROM
         |    base_data
         |GROUP BY unitid, planid, userid, industry, adslot_type, site_type, media
       """.stripMargin
    println(sqlRequest1)
    val data1 = spark.sql(sqlRequest1)

    // 预算数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`dt` = '$date1'"

    val sqlRequest2 =
      s"""
         |SELECT
         |    plan_id as planid,
         |    amt_budget as budget
         |FROM
         |    qttdw.dim_plan_ds
         |WHERE
         |    $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val data2 = spark.sql(sqlRequest2)


    // 是否oCPC单元
    val sqlRequest3 =
      s"""
         |SELECT
         |  unit_id as unitid,
         |  is_ocpc
         |FROM
         |  qttdw.dim_unit_ds
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest3)
    val data3 = spark.sql(sqlRequest3)

    // 数据关联
    val data = data1
        .join(data2, Seq("planid"), "left_outer")
        .join(data3, Seq("unitid"), "left_outer")
        .select("unitid", "planid", "userid", "industry", "adslot_type", "site_type", "media", "click", "show", "cv", "cost", "ocpc_cpagiven", "ocpc_cpareal", "ocpc_click", "ocpc_show", "ocpc_cv", "ocpc_cost", "hidden_cpagiven", "hidden_cpareal", "hidden_click", "hidden_show", "hidden_cv", "hidden_cost", "budget", "is_ocpc")
        .withColumn("date", lit(date1))

    data
  }

  def getOcpcLog(date: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias.total.media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 取历史数据
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)
    val selectCondition = s"`date` = '$date1'"

    // ctrData
    val sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    planid,
         |    userid,
         |    adslot_type,
         |    isclick,
         |    isshow,
         |    ocpc_log,
         |    ocpc_step,
         |    price,
         |    (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |    end) as industry,
         |    (case
         |        when media_appsid in ('80000001', '80000002') then 'qtt'
         |        when media_appsid = '80002819' then 'hottopic'
         |        else 'novel'
         |    end) as media,
         |    conversion_goal
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    $selectCondition
         |and $mediaSelection
         |and round(adclass/1000) != 132101  --去掉互动导流
         |and isshow = 1
         |and ideaid > 0
         |and adsrc = 1
         |and searchid is not null
         |and is_ocpc = 1
       """.stripMargin
    println(sqlRequest)
    val ctrBaseData = spark
        .sql(sqlRequest)
        .withColumn("cvr_goal", udfConcatStringInt("cvr")(col("conversion_goal")))
        .withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    ctrBaseData.createOrReplaceTempView("base_ctr")
    val sqlRequestCtr =
      s"""
         |SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    planid,
         |    userid,
         |    adslot_type,
         |    isclick,
         |    isshow,
         |    ocpc_step,
         |    (case
         |      when ocpc_step <= 1 then 0
         |      else cast(ocpc_log_dict['cpagiven'] as double)
         |    end) as cpagiven,
         |    price,
         |    industry,
         |    media,
         |    conversion_goal,
         |    cvr_goal
         |FROM
         |  base_ctr
       """.stripMargin
    val ctrData = spark.sql(sqlRequestCtr)

    // cvrData
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr,
         |  cvr_goal
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2).distinct()

    // 数据关联
    val resultDF = ctrData
      .join(cvrData, Seq("searchid", "cvr_goal"), "left_outer")
      .na.fill(0, Seq("iscvr"))

//    resultDF.repartition(50).write.mode("overwrite").saveAsTable("test.check_data_ocpc20190429new")

    resultDF

  }


}