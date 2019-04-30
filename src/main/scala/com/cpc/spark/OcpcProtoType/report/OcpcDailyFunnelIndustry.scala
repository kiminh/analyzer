package com.cpc.spark.OcpcProtoType.report

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.ocpc.OcpcUtils.getTimeRangeSql2
import com.cpc.spark.ocpcV3.utils
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable

object OcpcDailyFunnelIndustry {
  def main(args: Array[String]): Unit = {
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    println("parameters:")
    println(s"date=$date, hour=$hour, media=$media")

    // spark app name
    val spark = SparkSession.builder().appName(s"OcpcHourlyAucReport: $date, $hour").enableHiveSupport().getOrCreate()

    val rawData = getOcpcLog(media, date, hour, spark)

    val data1 = calculateBase(rawData, date, hour, spark)

    val result1 = data1
      .select("unitid", "planid", "userid", "click", "show", "cv", "cost", "ocpc_cpagiven", "ocpc_cpareal", "ocpc_click", "ocpc_show", "ocpc_cv", "ocpc_cost", "hidden_cpagiven", "hidden_cpareal", "hidden_click", "hidden_show", "hidden_cv", "hidden_cost", "budget", "industry", "date")

    result1
      .repartition(5)
      .write.mode("overwrite").insertInto("dl_cpc.ocpc_funnel_data_industry_daily")
//      .write.mode("overwrite").saveAsTable("test.ocpc_funnel_data_industry_daily")

    val data2 = calculateCnt(rawData, date, hour, spark)
    val result2 = data2
      .withColumn("ideaid_over_unitid", col("ideaid_cnt") * 1.0 / col("unitid_cnt"))
      .withColumn("ideaid_over_userid", col("ideaid_cnt") * 1.0 / col("userid"))
      .select("industry", "ideaid_cnt", "unitid_cnt", "userid_cnt", "ideaid_over_unitid", "ideaid_over_userid", "date")

    result2
      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.ocpc_funnel_ideaid_cnt_daily")


  }

  def calculateCnt(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {
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
         |    count(distinct ideaid) as ideaid_cnt,
         |    count(distinct unitid) as unitid_cnt,
         |    count(distinct userid) as userid_cnt
         |FROM
         |    base_data
         |GROUP BY industry
       """.stripMargin
    println(sqlRequest)
    val data = spark
        .sql(sqlRequest)
        .withColumn("date", lit(date1))

    data
  }

  def calculateBase(baseData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // 汇总数据
    baseData.createOrReplaceTempView("base_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |    unitid,
         |    planid,
         |    userid,
         |    industry,
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
         |GROUP BY unitid, planid, userid, industry
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

    // 数据关联
    val data = data1
        .join(data2, Seq("planid"), "left_outer")
        .select("unitid", "planid", "userid", "industry", "click", "show", "cv", "cost", "ocpc_cpagiven", "ocpc_cpareal", "ocpc_click", "ocpc_show", "ocpc_cv", "ocpc_cost", "hidden_cpagiven", "hidden_cpareal", "hidden_click", "hidden_show", "hidden_cv", "hidden_cost", "budget")
        .withColumn("date", lit(date1))

    data
  }

  def getOcpcLog(media: String, date: String, hour: String, spark: SparkSession) = {
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
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
         |    isclick,
         |    isshow,
         |    (case when ocpc_log = "" then "cpc"
         |        when ocpc_log != "" and ocpc_log like "%IsHiddenOcpc:0%" then "ocpc"
         |        when ocpc_log != "" and ocpc_log like "%IsHiddenOcpc:1%" then "hidden_ocpc"
         |        else "other" end) as cpc_type,
         |    (case when is_ocpc = 1 and ocpc_log != "" then regexp_extract(ocpc_log, 'cpagiven:(.*?),', 1) else 0 end) as cpagiven,
         |    price,
         |    is_ocpc,
         |    ocpc_log,
         |    (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |    end) as industry,
         |    is_api_callback
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    $selectCondition
         |and $mediaSelection
         |and round(adclass/1000) != 132101  --去掉互动导流
         |and isshow = 1
         |and ideaid > 0
         |and adsrc = 1
         |and adslot_type in (1,2,3)
         |and searchid is not null
         |and (charge_type = 1 or charge_type is null)
       """.stripMargin
    println(sqlRequest)
    val ctrBaseData = spark
      .sql(sqlRequest)

    ctrBaseData.createOrReplaceTempView("base_ctr")
    val sqlRequestCtr =
      s"""
         |SELECT
         |    searchid,
         |    ideaid,
         |    unitid,
         |    planid,
         |    userid,
         |    isclick,
         |    isshow,
         |    cpc_type,
         |    cpagiven,
         |    price,
         |    is_ocpc,
         |    ocpc_log,
         |    industry,
         |    is_api_callback,
         |    (case when is_api_callback = 1 and industry = 'feedapp' then 2
         |          when industry = 'elds' then 3
         |          else 0 end) as conversion_goal
         |FROM
         |  base_ctr
       """.stripMargin
    val ctrData = spark
      .sql(sqlRequestCtr)
      .filter(s"conversion_goal in (2, 3)")

//    ctrBaseData.repartition(50).write.mode("overwrite").saveAsTable("test.check_data_ocpc20190429")

    // cvr2Data
    val sqlRequest2 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr2
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = 'cvr2'
       """.stripMargin
    println(sqlRequest2)
    val cvr2Data = spark.sql(sqlRequest2).distinct()

    // cvr3Data
    val sqlRequest3 =
      s"""
         |SELECT
         |  searchid,
         |  1 as iscvr3
         |FROM
         |  dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |  $selectCondition
         |AND
         |  cvr_goal = 'cvr3'
       """.stripMargin
    println(sqlRequest3)
    val cvr3Data = spark.sql(sqlRequest3).distinct()

    // 数据关联
    val resultDF = ctrData
      .join(cvr2Data, Seq("searchid"), "left_outer")
      .join(cvr3Data, Seq("searchid"), "left_outer")
      .withColumn("iscvr", when(col("conversion_goal") === 2, col("iscvr2")).otherwise(col("iscvr3")))
      .na.fill(0, Seq("iscvr"))

    resultDF

  }



}