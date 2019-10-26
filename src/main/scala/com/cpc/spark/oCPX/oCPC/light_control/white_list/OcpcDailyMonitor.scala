package com.cpc.spark.oCPX.oCPC.light_control.white_list

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.{DataFrame, SparkSession}


object OcpcDailyMonitor {
  def main(args: Array[String]): Unit = {
    /*
    新版报表程序
    1. 从基础数据报表抽取展点消、转化等数据
    2. 按照相关数据报表进行关联，过滤掉无关的单元，剩下的单元
    3. 按照单元、媒体维度计算赔付比例与成本控制情况
     */
    val spark = SparkSession
      .builder()
      .appName("OcpcHourlyReport")
      .enableHiveSupport()
      .getOrCreate()

    val date = args(0).toString
    println("parameters:")
    println(s"date=$date")

    // 拉取点击、消费、转化等基础数据
    val rawData = getBaseReportData(date, spark)

    // 拉取实验单元
    val expUnits = getExpUnitList(date, spark)

    // 关联抽取实验数据
    val expData = getExpData(rawData, expUnits, date, spark)

    expData
      .repartition(1)
      .write.mode("overwrite").saveAsTable("test.check_ocpc_exp_data20191029")

  }

  def getExpData(rawData: DataFrame, expUnitsRaw: DataFrame, date: String, spark: SparkSession) = {
    val expUnits = expUnitsRaw
      .filter(s"ocpc_date = '$date'")

    val data = rawData
      .join(expUnits, Seq("unitid", "userid", "conversion_goal"), "inner")
      .cache()

    data.show(10)
    data
  }

  def getExpUnitList(date: String, spark: SparkSession) = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd")
    val today = dateConverter.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -1)
    val yesterday = calendar.getTime
    val date1 = dateConverter.format(yesterday)

    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    conversion_goal,
         |    last_ocpc_opentime,
         |    create_time,
         |    date(last_ocpc_opentime) as ocpc_date
         |FROM
         |    (select unitid, userid, conversion_goal
         |    from dl_cpc.ocpc_auto_second_stage_hourly
         |    where date >= '$date1'
         |    group by unitid, userid, conversion_goal) as t1
         |INNER JOIN
         |    (SELECT
         |        unit_id as unitid,
         |        aduser as userid,
         |        is_ocpc,
         |        conversion_goal,
         |        last_ocpc_opentime,
         |        create_time
         |    from
         |        qttdw.dim_unit_ds
         |    WHERE
         |        dt = '$date'
         |    and
         |        is_ocpc = 1
         |    and create_time >= '$date1') as t2
         |ON
         |    t1.unitid = t2.unitid
         |AND
         |    t1.userid = t2.userid
         |AND
         |    t1.conversion_goal = t2.conversion_goal
         |""".stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
  }

  def getBaseReportData(date: String, spark: SparkSession) = {
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    media,
         |    industry,
         |    conversion_goal,
         |    sum(show) as show,
         |    sum(click) as click,
         |    sum(cv) as cv,
         |    sum(total_prectr) * 1.0 / sum(show) as pre_ctr,
         |    sum(click) * 1.0 / sum(show) as post_ctr,
         |    sum(total_precvr) * 1.0 / sum(click) as pre_cvr,
         |    sum(total_calipcvr) * 1.0 / sum(click) as cali_precvr,
         |    sum(cv) * 1.0 / sum(click) as post_cvr,
         |    sum(total_price) * 0.01 as cost,
         |    (case
         |      when sum(total_price) <= 1.2 * sum(cv) * sum(total_cpagiven) / sum(click) then 0
         |      else sum(total_price) - 1.2 * sum(cv) * sum(total_cpagiven) / sum(click)
         |    end) * 0.01 as pay,
         |    (case
         |      when sum(cv) * sum(total_cpagiven) / sum(click) <= sum(total_price) then 0
         |      else sum(cv) * sum(total_cpagiven) / sum(click) - sum(total_price)
         |    end) * 0.01 as buffer,
         |    sum(total_price) * 1.0 / sum(click) as acp,
         |    sum(total_bid) * 1.0 / sum(click) as acb,
         |    sum(total_cpagiven) * 1.0 / sum(click) as cpagiven,
         |    sum(total_price) * 1.0 / sum(cv) as cpareal,
         |    sum(total_jfbfactor) * 1.0 / sum(click) as jfb_factor,
         |    sum(total_cvrfactor) * 1.0 / sum(click) as cvr_factor
         |FROM
         |    dl_cpc.ocpc_report_industry_hourly
         |WHERE
         |    `date` = '$date'
         |GROUP BY unitid, userid, media, industry, conversion_goal
       """.stripMargin
    println(sqlRequest)
    val data = spark.sql(sqlRequest)
    data
  }

}