package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.udfs.Udfs_wj.udfStringToMap
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, sum, when}


object OcpcSuggestCPA {
  def main(args: Array[String]): Unit = {
    /*
    新版推荐cpa程序：
    unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, cal_bid, auc, kvalue, industry, is_recommend, ocpc_flag, usertype, pcoc1, pcoc2

    主要源表：dl_cpc.ocpc_base_unionlog, dl_cpc.ocpc_label_cvr_hourly

    数据构成分为以下部分:
    1. 基础数据部分：unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, industry, usertype
    2. ocpc部分：kvalue
    3. 模型部分：auc
    4. 实时查询：ocpc_flag
    5. 历史推荐cpa数据：pcoc1, pcoc2
    6.
     */
    // 计算日期周期
    val date = args(0).toString
    val hour = args(1).toString
    val media = args(2).toString
    val cvrGoal = "cvr1"
    val version = "qtt_demo"
    val spark = SparkSession
      .builder()
      .appName(s"ocpc suggest cpa v2: $date, $hour")
      .enableHiveSupport().getOrCreate()

    println("parameters:")
    println(s"date=$date, hour=$hour, media=$media, cvrGoal=$cvrGoal, version=$version")


    // 取基础数据部分
    val baseData = getBaseData(media, cvrGoal, date, hour, spark)
    baseData.write.mode("overwrite").saveAsTable("test.check_ocpc_suggest_data20190306a")

    // 模型部分

    // 实时查询ocpc标记（从mysql抽取）

    // 历史推荐cpa的pcoc数据
  }

  def getBaseData(media: String, cvrGoal: String, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取基础数据部分：unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, industry, usertype
     */
    // 按照转化目标抽取基础数据表
    val baseLog = getBaseLog(media, cvrGoal, date, hour, spark)

    // 统计数据
    val resultDF = calculateLog(baseLog, date, hour, spark)

    resultDF
  }

  def calculateLog(data: DataFrame, date: String, hour: String, spark: SparkSession) = {
    // 抽取基础数据
    data.createOrReplaceTempView("base_data")
    val sqlRequest =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    adclass,
         |    isshow,
         |    isclick,
         |    price,
         |    bid as original_bid,
         |    ocpc_log,
         |    industry,
         |    usertype,
         |    exp_cvr,
         |    exp_ctr,
         |    ocpc_log_dict,
         |    iscvr,
         |    (case when length(ocpc_log) > 0 then cast(ocpc_log_dict['dynamicbid'] as double)
         |          else cast(bid as double) end) as real_bid
         |FROM
         |    base_data
       """.stripMargin
    println(sqlRequest)
    val rawData = spark.sql(sqlRequest)

    // 数据统计: unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, industry, usertype
    // 统计指标：unitid, userid, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa
    val dataPart1 = calculateDataPart1(rawData, date, hour, spark)

    // 统计指标：unitid, pcvr, post_cvr, pcoc
    val dataPart2 = calculateDataPart2(rawData, date, hour, spark)

    // 统计指标：unitid, userid, adclass, industry, usertype
    val dataPart3 = calculateDataPart3(rawData, date, hour, spark)

    // 数据关联
    val resultDF = dataPart1
      .join(dataPart2, Seq("unitid"), "left_outer")
      .join(dataPart3, Seq("unitid"), "left_outer")
      .select("unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "industry", "usertype")

    resultDF
  }

  def calculateDataPart3(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    /*
    统计指标：unitid, userid, adclass, industry, usertype
     */
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |    tt.unitid,
         |    tt.industry
         |FROM
         |    (SELECT
         |        t.unitid,
         |        t.industry,
         |        t.cnt,
         |        row_number() over(partition by t.unitid order by t.cnt desc) as seq
         |    FROM
         |        (select
         |            unitid,
         |            industry,
         |            count(distinct searchid) as cnt
         |        from raw_data
         |        WHERE
         |            isclick=1
         |        group by unitid, industry) as t) as tt
         |WHERE
         |    tt.seq=1
       """.stripMargin
    println(sqlRequest1)
    val industryData = spark.sql(sqlRequest1)

    val sqlRequest2 =
      s"""
         |SELECT
         |    tt.unitid,
         |    tt.adclass
         |FROM
         |    (SELECT
         |        t.unitid,
         |        t.adclass,
         |        t.cnt,
         |        row_number() over(partition by t.unitid order by t.cnt desc) as seq
         |    FROM
         |        (select
         |            unitid,
         |            adclass,
         |            count(distinct searchid) as cnt
         |        from raw_data
         |        WHERE
         |            isclick=1
         |        group by unitid, adclass) as t) as tt
         |WHERE
         |    tt.seq=1
       """.stripMargin
    println(sqlRequest2)
    val adclassData = spark.sql(sqlRequest2)

    val sqlRequest3 =
      s"""
         |SELECT
         |    tt.unitid,
         |    tt.usertype
         |FROM
         |    (SELECT
         |        t.unitid,
         |        t.usertype,
         |        t.cnt,
         |        row_number() over(partition by t.unitid order by t.cnt desc) as seq
         |    FROM
         |        (select
         |            unitid,
         |            usertype,
         |            count(distinct searchid) as cnt
         |        from raw_data
         |        WHERE
         |            isclick=1
         |        group by unitid, usertype) as t) as tt
         |WHERE
         |    tt.seq=1
       """.stripMargin
    println(sqlRequest3)
    val usertypeData = spark.sql(sqlRequest3)

    val resultDF = industryData
      .join(adclassData, Seq("unitid"), "outer")
      .join(usertypeData, Seq("unitid"), "outer")
      .select("unitid", "adclass", "industry", "usertype")

    resultDF
  }

  def calculateDataPart2(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val data = rawData
      .groupBy("unitid")
      .agg(
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("conversion")
      )
      .withColumn("post_cvr", col("conversion") * 1.0 / col("click"))
      .withColumn("post_cvr_cali", col("post_cvr") * 5.0)
      .select("unitid", "post_cvr", "post_cvr_cali")

    val caliData = rawData
      .join(data, Seq("unitid"), "left_outer")
      .select("searchid", "unitid", "exp_cvr", "isclick", "iscvr", "post_cvr", "post_cvr_cali")
      .withColumn("pre_cvr", when(col("exp_cvr")> col("post_cvr_cali"), col("post_cvr_cali")).otherwise(col("exp_cvr")))
      .select("searchid", "unitid", "exp_cvr", "isclick", "iscvr", "post_cvr", "pre_cvr", "post_cvr_cali")

    val finalData = caliData
      .groupBy("unitid")
      .agg(
        sum(col("pre_cvr")).alias("pre_cvr"),
        sum(col("exp_cvr")).alias("exp_cvr"),
        sum(col("isclick")).alias("click"),
        sum(col("iscvr")).alias("conversion")
      )
      .withColumn("pre_cvr", col("pre_cvr") * 1.0 / col("click"))
      .withColumn("exp_cvr", col("exp_cvr") * 1.0 / col("click"))
      .select("unitid", "pre_cvr", "exp_cvr", "click", "conversion")

    val resultDF = finalData
      .join(data, Seq("unitid"), "outer")
      .withColumn("pcoc", col("pre_cvr") * 1.0 / col("post_cvr"))
      .withColumn("pcvr", col("pre_cvr"))
      .select("unitid", "exp_cvr", "pre_cvr", "post_cvr", "pcvr", "pcoc")

    resultDF
  }

  def calculateDataPart1(rawData: DataFrame, date: String, hour: String, spark: SparkSession) = {
    rawData.createOrReplaceTempView("raw_data")
    val sqlRequest1 =
      s"""
         |SELECT
         |  unitid,
         |  userid,
         |  sum(isshow) as show,
         |  sum(isclick) as click,
         |  sum(iscvr) as cvrcnt,
         |  sum(case when isclick=1 then price else 0 end) as cost,
         |  sum(isclick) * 1.0 / sum(isshow) as post_ctr,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(isclick) as acp,
         |  sum(case when isclick=1 then real_bid else 0 end) * 1.0 / sum(isclick) as acb,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(case when isclick=1 then real_bid else 0 end) as jfb,
         |  sum(case when isclick=1 then price else 0 end) * 1.0 / sum(iscvr) as cpa
         |FROM
         |  raw_data
         |GROUP BY unitid, userid
       """.stripMargin
    println(sqlRequest1)
    val data = spark.sql(sqlRequest1)

    data
  }

  def getBaseLog(media: String, cvrType: String, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取基础数据用于后续计算与统计
    unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, industry, usertype
     */
    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 时间区间选择
    val hourCnt = 72
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val endDay = date + " " + hour
    val endDayTime = dateConverter.parse(endDay)
    val calendar = Calendar.getInstance
    calendar.setTime(endDayTime)
    calendar.add(Calendar.HOUR, -hourCnt)
    val startDateTime = calendar.getTime
    val startDateStr = dateConverter.format(startDateTime)
    val date1 = startDateStr.split(" ")(0)
    val hour1 = startDateStr.split(" ")(1)
    val timeSelection = getTimeRangeSql(date1, hour1, date, hour)

    // 抽取点击数据: dl_cpc.ocpc_base_unionlog
    val sqlRequest1 =
      s"""
         |SELECT
         |    searchid,
         |    unitid,
         |    userid,
         |    adclass,
         |    isshow,
         |    isclick,
         |    price,
         |    bid,
         |    ocpc_log,
         |    (case
         |        when (cast(adclass as string) like '134%' or cast(adclass as string) like '107%') then "elds"
         |        when (adslot_type<>7 and cast(adclass as string) like '100%') then "feedapp"
         |        when (adslot_type=7 and cast(adclass as string) like '100%') then "yysc"
         |        when adclass in (110110100, 125100100) then "wzcp"
         |        else "others"
         |    end) as industry,
         |    usertype,
         |    exp_cvr,
         |    exp_ctr
         |FROM
         |    dl_cpc.ocpc_base_unionlog
         |WHERE
         |    $timeSelection
         |AND
         |    $mediaSelection
         |AND
         |    antispam = 0
         |AND
         |    adslot_type in (1,2,3)
         |AND
         |    adsrc = 1
         |AND
         |    (charge_type is null or charge_type = 1)
       """.stripMargin
    println(sqlRequest1)
    val ctrData = spark.sql(sqlRequest1).withColumn("ocpc_log_dict", udfStringToMap()(col("ocpc_log")))

    // 抽取转化数据
    val sqlRequest2 =
      s"""
         |SELECT
         |    searchid,
         |    label as iscvr
         |FROM
         |    dl_cpc.ocpc_label_cvr_hourly
         |WHERE
         |    `date` >= '$date1'
         |AND
         |    cvr_goal = '$cvrType'
       """.stripMargin
    println(sqlRequest2)
    val cvrData = spark.sql(sqlRequest2)

    // 数据关联
    val data = ctrData
      .join(cvrData, Seq("searchid"), "left_outer")

    data.show(10)
    data
  }

//  def getTimeRangeSqlCondition(endDate: String, endHour: String, hourCnt: Int): String = {
//    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
//    val endDay = endDate + " " + endHour
//    val endDayTime = dateConverter.parse(endDay)
//    val calendar = Calendar.getInstance
//    calendar.setTime(endDayTime)
//    calendar.add(Calendar.HOUR, -hourCnt)
//    val startDateTime = calendar.getTime
//    val startDateStr = dateConverter.format(startDateTime)
//    val startDate = startDateStr.split(" ")(0)
//    val startHour = startDateStr.split(" ")(1)
//    val timeSelection = getTimeRangeSql(startDate, startHour, endDate, endHour)
//    println(s"time selection is: $timeSelection")
//    return timeSelection
//  }

  def getTimeRangeSql(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`date` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`date` = '$startDate' and hour > '$startHour') " +
      s"or (`date` = '$endDate' and hour <= '$endHour') " +
      s"or (`date` > '$startDate' and `date` < '$endDate'))"
  }
}
