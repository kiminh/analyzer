package com.cpc.spark.ml.dnn.Utils

import java.text.SimpleDateFormat
import java.util.Calendar

object DateUtils {

  val datekeyDateFormat = new SimpleDateFormat("yyyyMMdd")
  val dateDateFormat = new SimpleDateFormat("yyyy-MM-dd")

  def datekeyToDate(x:String): String ={
    dateDateFormat.format(datekeyDateFormat.parse(x))
  }

  def getPrevDate(dateStr: String, prevPeriod: Int): String = {
    val cal = Calendar.getInstance()
    cal.setTime(dateDateFormat.parse(dateStr))
    cal.add(Calendar.DATE, -prevPeriod)
    dateDateFormat.format(cal.getTime)
  }

  def getHourToday() = {

    // 获取当前的日历时间
    val c = Calendar.getInstance()

    // 获取月
    val hour = c.get(Calendar.HOUR_OF_DAY)
    hour
  }

  def main(args: Array[String]): Unit = {
    println(getPrevDate("2019-08-02",-1))
  }
}
