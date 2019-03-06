package com.cpc.spark.OcpcProtoType.suggest_cpa_qtt

import java.text.SimpleDateFormat
import java.util.Calendar
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession


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
    val version = "qtt_demo"
    val spark = SparkSession
      .builder()
      .appName(s"ocpc suggest cpa v2: $date, $hour")
      .enableHiveSupport().getOrCreate()


    // 取基础数据部分
    val baseData = getBaseData(media, date, hour, spark)

    // 模型部分

    // 实时查询ocpc标记（从mysql抽取）

    // 历史推荐cpa的pcoc数据
  }

  def getBaseData(media: String, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取基础数据部分：unitid, userid, adclass, original_conversion, conversion_goal, show, click, cvrcnt, cost, post_ctr, acp, acb, jfb, cpa, pcvr, post_cvr, pcoc, industry, usertype
     */
    val baseLog = getBaseLog(media, "cvr1", date, hour, spark)
    baseLog
  }

  def getBaseLog(media: String, cvrType: String, date: String, hour: String, spark: SparkSession) = {
    /*
    抽取基础数据用于后续计算与统计
     */
    // 媒体选择
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)

    // 时间区间选择
    val hourCnt = 72
    val selectCondition = getTimeRangeSqlCondition(date, hour, hourCnt)

    // 抽取点击数据: dl_cpc.ocpc_base_unionlog
  }

  def getTimeRangeSqlCondition(endDate: String, endHour: String, hourCnt: Int): String = {
    val dateConverter = new SimpleDateFormat("yyyy-MM-dd HH")
    val endDay = endDate + " " + endHour
    val endDayTime = dateConverter.parse(endDay)
    val calendar = Calendar.getInstance
    calendar.setTime(endDayTime)
    calendar.add(Calendar.HOUR, -hourCnt)
    val startDateTime = calendar.getTime
    val startDateStr = dateConverter.format(startDateTime)
    val startDate = startDateStr.split(" ")(0)
    val startHour = startDateStr.split(" ")(1)
    val timeSelection = getTimeRangeSql(startDate, startHour, endDate, endHour)
    println(s"time selection is: $timeSelection")
    return timeSelection
  }

  def getTimeRangeSql(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`date` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`date` = '$startDate' and hour > '$startHour') " +
      s"or (`date` = '$endDate' and hour <= '$endHour') " +
      s"or (`date` > '$startDate' and `date` < '$endDate'))"
  }
}
