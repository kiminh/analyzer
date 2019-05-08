package com.cpc.spark.OcpcProtoType.suggest_cpa_v1

import java.text.SimpleDateFormat
import java.util.Calendar

import com.cpc.spark.tools.testOperateMySQL

//import org.apache.spark.sql.functions.udf
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._


object OcpcSuggestReport {
  def main(args: Array[String]): Unit = {
    /*
    将推荐cpa的数据推送到bi报表上：
    1. 整理准入名单
    2. 整理不准入名单
    3. 推送数据进入report
     */
    Logger.getRootLogger.setLevel(Level.WARN)
    val date = args(0).toString
    val hour = args(1).toString
    val version = args(2).toString
    val media = args(3).toString
    val spark = SparkSession
      .builder()
      .appName(s"ocpc suggest report: $date, $hour, $version, $media")
      .enableHiveSupport().getOrCreate()

    val filterCondition = s"((industry = 'elds' and conversion_goal = 3) or (industry = 'feedapp' and conversion_goal in (1, 2)))"

    // 抽取每个unitid的adslot_type
    val unitAdslotType = getUnitAdslotType(media, date, hour, spark)

    // 整理准入名单
    val permitUnit = getPermitUnit(unitAdslotType, filterCondition, version, date, hour, spark)
    permitUnit.repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_suggest_cpa_permit_unit")

    // 整理不准入名单
    val unpermitUnit = getUnpermitUnit(unitAdslotType, filterCondition, version, date, hour, spark)
    unpermitUnit.repartition(5).write.mode("overwrite").saveAsTable("test.ocpc_suggest_cpa_unpermit_unit")

    saveDataToMysql(permitUnit, unpermitUnit, date, hour, spark)
  }

  def saveDataToMysql(permitUnit: DataFrame, unpermitUnit: DataFrame, date: String, hour: String, spark: SparkSession) = {
    val hourInt = hour.toInt
    // 准入表
    val dataUnitMysql1 = permitUnit
      .select("unitid", "userid", "adclass", "industry", "cv_goal", "adslot_type", "show", "click", "cv", "charge", "cpm", "suggest_cpa", "is_ocpc", "usertype")
      .na.fill("", Seq("adslot_type"))
      .na.fill(0, Seq("show", "click", "cv", "charge", "cpm", "suggest_cpa", "is_ocpc", "usertype"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hourInt))
    val reportTableUnit1 = "report2.report_ocpc_suggest_list"
    val delSQLunit1 = s"delete from $reportTableUnit1 where `date` = '$date' and hour = $hourInt"

    testOperateMySQL.update(delSQLunit1) //先删除历史数据
    testOperateMySQL.insert(dataUnitMysql1, reportTableUnit1) //插入数据

    // 未准入表
    val dataUnitMysql2 = unpermitUnit
      .select("unitid", "userid", "adclass", "industry", "cv_goal", "adslot_type", "show", "click", "cv", "charge", "auc", "acb", "cal_bid", "cpa", "pcvr", "pcoc", "jfb", "no_suggest_cpa_reason")
      .na.fill("", Seq("adslot_type", "no_suggest_cpa_reason"))
      .na.fill(0, Seq("show", "click", "cv", "charge", "auc", "acb", "cal_bid", "cpa", "pcvr", "kvalue", "pcoc", "jfb"))
      .withColumn("date", lit(date))
      .withColumn("hour", lit(hourInt))
    val reportTableUnit2 = "report2.report_ocpc_no_suggest_list"
    val delSQLunit2 = s"delete from $reportTableUnit2 where `date` = '$date' and hour = $hourInt"

    testOperateMySQL.update(delSQLunit2) //先删除历史数据
    testOperateMySQL.insert(dataUnitMysql2, reportTableUnit2) //插入数据
  }

  def getUnpermitUnit(adslotTypes: DataFrame, filterCondition: String, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    title = ",".join(["unitid", "userid", "adclass", "industry", "cv_goal", "adslot_type", \
                      "show", "click", "cv", "charge(yuan)", "auc", "acb", "cal_bid", "cpa", "pcvr", "kvalue", "pcoc", "jfb", \
                      "no_suggest_cpa_reason", "owner"])
     */
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    adclass,
         |    industry,
         |    original_conversion as cv_goal,
         |    show,
         |    click,
         |    cvrcnt as cv,
         |    cost as charge,
         |    auc,
         |    acb,
         |    cal_bid,
         |    cpa,
         |    pcvr,
         |    kvalue,
         |    pcoc,
         |    jfb,
         |    ocpc_flag,
         |    cal_bid * 1.0 / acb as bid_ratio
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  is_recommend = 1
         |AND
         |  $filterCondition
       """.stripMargin
    println(sqlRequest)

    val rawData = spark.sql(sqlRequest).na.fill(0.0, Seq("bid_ratio"))

    val resultDF = rawData
      .join(adslotTypes, Seq("unitid"), "left_outer")
      .withColumn("adslot_type", udfAdslotTypeMap()(col("adslot_type")))
      .withColumn("no_suggest_cpa_reason", udfNoSuggestReason()(col("cv"), col("auc"), col("ocpc_flag"), col("bid_ratio")))
      .select("unitid", "userid", "adclass", "industry", "cv_goal", "adslot_type", "show", "click", "cv", "charge", "auc", "acb", "cal_bid", "cpa", "pcvr", "kvalue", "pcoc", "jfb", "no_suggest_cpa_reason")

    resultDF

  }

  def udfNoSuggestReason() = udf((cv: Int, auc: Double, ocpc_flag: Int, bid_ratio: Double) => {
    /*
    if ocpc_flag == "1":
        reason = "ocpc already open"
    elif auc == "NULL" or auc == "":
        reason = "auc is NULL"
        owner = "xieyufei"
    elif float(auc) < 0.65:
        reason = "auc < 0.65"
        owner = "xieyufei"
    elif cv == "NULL" or cv == "":
        reason = "cv is NULL"
    elif float(cv) < 40:
        reason = "cv < 40"
    elif bid_ratio < 0.7 or bid_ratio > 1.3:
        reason = "cal_bid no_ok"
     */
    var result = ""
    if (ocpc_flag == 1) {
      result = "ocpc already open"
    } else if (auc == null) {
      result = "auc is null"
    } else if (auc < 0.65) {
      result = "auc < 0.65"
    } else if (cv == null) {
      result = "cv is null"
    } else if (cv < 40) {
      result = "cv < 40"
    } else if (bid_ratio < 0.7 || bid_ratio > 1.3) {
      result = "cal_bid not ok"
    } else {
      result = "other"
    }

    result
  })


  def getUnitAdslotType(media: String, date: String, hour: String, spark: SparkSession) = {
    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 时间区间选择
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val endDay = date + " " + hour
    val endDayTime = dateConverter.parse(endDay)
    val calendar = Calendar.getInstance
    calendar.setTime(endDayTime)
    calendar.add(Calendar.HOUR, -72)
    val startDateTime = calendar.getTime
    val startDateStr = dateConverter.format(startDateTime)
    val date1 = startDateStr.split(" ")(0)
    val hour1 = startDateStr.split(" ")(1)

    val sqlRequest =
      s"""
         |SELECT
         |  unitid,
         |  adslot_type
         |FROM
         |  dl_cpc.ocpc_ctr_data_hourly
         |WHERE
         |  `date` between '$date1' and '$date'
         |AND
         |  $mediaSelection
         |GROUP BY unitid, adslot_type
       """.stripMargin
    println(sqlRequest)
    val resultDF = spark.sql(sqlRequest)

    resultDF
  }

  def getPermitUnit(adslotTypes: DataFrame, filterCondition: String, version: String, date: String, hour: String, spark: SparkSession) = {
    /*
    "unitid", "userid", "adclass", "original_conversion", "conversion_goal", "show", "click", "cvrcnt", "cost", "post_ctr", "acp", "acb", "jfb", "cpa", "pcvr", "post_cvr", "pcoc", "cal_bid", "auc", "kvalue", "industry", "is_recommend", "ocpc_flag", "usertype", "pcoc1", "pcoc2", "zerobid_percent", "bottom_halfbid_percent", "top_halfbid_percent", "largebid_percent"

    slottype_dic = {"1": "list_page", "2": "details_page", "3": "interaction", \
                    "4": "open_screen", "5": "banner", "6": "video", "7": "incentive"}
    title = ",".join(["unitid", "userid", "adclass", "industry", "cv_goal", "adslot_type", \
                      "show", "click", "cv", "charge(yuan)", "cpm(yuan)", "suggest_cpa(yuan)", "is_ocpc", "usertype",
                      "pcoc1", "pcoc2"])
     */
    val sqlRequest =
      s"""
         |SELECT
         |    unitid,
         |    userid,
         |    adclass,
         |    industry,
         |	   original_conversion as cv_goal,
         |	   show,
         |    click,
         |    cvrcnt as cv,
         |    cost as charge,
         |	   cost / 100.0 / (show / 1000.0) as cpm,
         |    cpa as suggest_cpa,
         |    ocpc_flag as is_ocpc,
         |    usertype
         |FROM
         |  dl_cpc.ocpc_suggest_cpa_recommend_hourly
         |WHERE
         |  `date` = '$date'
         |AND
         |  `hour` = '$hour'
         |AND
         |  version = '$version'
         |AND
         |  is_recommend = 1
         |AND
         |  $filterCondition
       """.stripMargin
    println(sqlRequest)

    val rawData = spark.sql(sqlRequest)

    val resultDF = rawData
      .join(adslotTypes, Seq("unitid"), "left_outer")
      .withColumn("adslot_type", udfAdslotTypeMap()(col("adslot_type")))
      .select("unitid", "userid", "adclass", "industry", "cv_goal", "adslot_type", "show", "click", "cv", "charge", "cpm", "suggest_cpa", "is_ocpc", "usertype")

    resultDF
  }

  def udfAdslotTypeMap() = udf((adslotType: Int) => {
    /*
    slottype_dic = {"1": "list_page", "2": "details_page", "3": "interaction", \
                    "4": "open_screen", "5": "banner", "6": "video", "7": "incentive"}
     */
    val result = adslotType match {
      case 1 => "list_page"
      case 2 => "details_page"
      case 3 => "interaction"
      case 4 => "open_screen"
      case 5 => "banner"
      case 6 => "video"
      case 7 => "incentive"
      case _ => "unknown"
    }
    result
  })
}
