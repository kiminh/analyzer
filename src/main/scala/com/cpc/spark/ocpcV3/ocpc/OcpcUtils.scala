package com.cpc.spark.ocpcV3.ocpc

import org.apache.spark.sql.SparkSession
import java.util.Properties

import com.typesafe.config.ConfigFactory


object OcpcUtils {
  def getIdentifierSql() = {
    val conf = ConfigFactory.load("ocpc")

    val identifiers = conf.getStringList("ocpc_all.cpa_history.identifier")
    var selectionID = ""
    for (item <- identifiers) {
      if (selectionID.length() > 0) {
        selectionID = selectionID + ", " + item
      }
    }
    selectionID
  }

  def getIdentifierString() = {
    val conf = ConfigFactory.load("ocpc")

    val identifiers = conf.getStringList("ocpc_all.cpa_history.identifier")
    var selectionID = ""
    for (item <- identifiers) {
      if (selectionID.length() > 0) {
        selectionID = selectionID + "|" + item
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
