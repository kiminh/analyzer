package com.cpc.spark.ocpcV3.ocpc

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.SparkSession
import java.util.Properties

import com.typesafe.config.ConfigFactory


object OcpcUtils {
  def getIdentifierSql() = {
    val confList = ConfigFactory
      .load("ocpc")
      .getStringList("ocpc_all.ocpc_regression.identifier")

    var selectionID = ""
    for (i <- 0 until confList.size()) {
      if (selectionID.length() > 0) {
        selectionID = selectionID + ", " + confList.get(i)
      }
    }
    println(s"####### $selectionID ##############")
    selectionID
  }

  def getIdentifierString() = {
    val confList = ConfigFactory
      .load("ocpc")
      .getStringList("ocpc_all.ocpc_regression.identifier")

    var selectionID = ""
    for (i <- 0 until confList.size()) {
      if (selectionID.length() > 0) {
        selectionID = selectionID + "|" + confList.get(i)
      }
    }
    println(s"####### $selectionID ##############")
    selectionID
  }

  def getTimeRangeSql2(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`date` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`date` = '$startDate' and hour > '$startHour') " +
      s"or (`date` = '$endDate' and hour <= '$endHour') " +
      s"or (`date` > '$startDate' and `date` < '$endDate'))"
  }

  def getTimeRangeSql3(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`dt` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`dt` = '$startDate' and hour > '$startHour') " +
      s"or (`dt` = '$endDate' and hour <= '$endHour') " +
      s"or (`dt` > '$startDate' and `dt` < '$endDate'))"
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
    return getTimeRangeSql2(startDate, startHour, endDate, endHour)
  }
}
