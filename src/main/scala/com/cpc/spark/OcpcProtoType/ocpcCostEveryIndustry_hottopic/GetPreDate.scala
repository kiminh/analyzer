package com.cpc.spark.OcpcProtoType.ocpcCostEveryIndustry_hottopic

import java.text.SimpleDateFormat
import java.util.Calendar

object GetPreDate {
  def getPreDate(date: String, n: Int = 1): String ={
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val today = sdf.parse(date)
    val calendar = Calendar.getInstance
    calendar.setTime(today)
    calendar.add(Calendar.DATE, -n)
    val yesterday = sdf.format(calendar.getTime)
    yesterday
  }
}
