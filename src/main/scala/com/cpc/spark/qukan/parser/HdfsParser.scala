package com.cpc.spark.qukan.parser

import java.text.SimpleDateFormat
import java.util.Calendar
import userprofile.Userprofile


/**
  * Created by Roy on 2017/4/14.
  */
object HdfsParser {

  val columnSep = '\001'

  def parseTextRow(row: String): (Boolean, String, Userprofile.UserProfile) = {
    val data = row.split(columnSep)
    if (data.length < 6) {
      (false, None, None)
    }

    val devid = data(0).trim
    if (devid.length == 0) {
      (false, None, None)
    }

    val coin = toInt(data(2))

    // 1 男  2 女
    val sex = toInt(data(3))

    val age = getAge(data(4))

    val profile = Userprofile
      .UserProfile
      .newBuilder()
      .setAge(age)
      .setSex(sex)
      .setCoin(coin)
      .build()

    (true, devid + "_UPDATA", profile)
  }

  //年龄 0: 未知 1: 小于18 2:18-23 3:24-30 4:31-40 5:41-50 6: >50
  def getAge(birth: String): Int = {
    if (birth.length != 10) {
      0
    }
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    try {
      cal.setTime(dateFormat.parse(birth))
    } catch {
      case e: Exception => 0
    }
    val ms = cal.getTimeInMillis()
    val days = ((System.currentTimeMillis() - ms) / 1000 * 3600 * 24).toInt
    if (days < 1) {
      0
    } else if (days < 18) {
      1
    } else if (days < 23) {
      2
    } else if (days < 30) {
      3
    } else if (days < 40) {
      4
    } else if (days < 50) {
      5
    } else {
      6
    }
  }

  def toInt(s: String): Int = {
    try {
      s.trim.toInt
    } catch {
      case e : Exception => 0
    }
  }
}

