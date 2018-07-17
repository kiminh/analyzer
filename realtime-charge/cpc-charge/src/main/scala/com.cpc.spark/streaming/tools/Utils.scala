package com.cpc.spark.streaming.tools

import java.text.SimpleDateFormat
import java.util.Date
object Utils {
  def formate_time(time_o: Long): Long = {
    if (time_o == 0) {
      return time_o
    }
    if (time_o.toString.length() <= 10) {
      return time_o
    } else {
      return time_o.toString.substring(0, 10).toLong
    }
  }
  val format1 = new SimpleDateFormat("yyyy-MM-dd");
  
  def get_date_hour_from_time(time_o : Long) : (String,Int) = {
    val date_o = new Date(time_o)
    val hour =  date_o.getHours
    var date = format1.format(date_o)
    return (date,hour)
  }
}