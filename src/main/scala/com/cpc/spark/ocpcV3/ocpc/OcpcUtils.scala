package com.cpc.spark.ocpcV3.ocpc

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
}
