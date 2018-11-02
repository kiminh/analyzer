package com.cpc.spark.ocpc

object OcpcUtils {
  def getTimeRangeSql2(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(`date` = '$startDate' and hour <= '$endHour' and hour > '$startHour')"
    }
    return s"((`date` = '$startDate' and hour > '$startHour') " +
      s"or (`date` = '$endDate' and hour <= '$endHour') " +
      s"or (`date` > '$startDate' and `date` < '$endDate'))"
  }
}