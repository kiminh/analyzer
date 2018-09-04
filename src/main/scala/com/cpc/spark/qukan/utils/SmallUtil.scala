package com.cpc.spark.qukan.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.functions.udf

object SmallUtil {


  def getDayBefore(date: String, before: Int): String = {
    val calendar = Calendar.getInstance
    //    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    calendar.setTime(dateFormat.parse(date))
    calendar.add(Calendar.DATE, -before)
    dateFormat.format(calendar.getTime)
  }

  def udfParseDayOfWeek = udf((logtime: String) => {
    val calendar = Calendar.getInstance
    //    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    calendar.setTime(dateFormat.parse(logtime))
    calendar.get(Calendar.DAY_OF_WEEK)
  })

}
