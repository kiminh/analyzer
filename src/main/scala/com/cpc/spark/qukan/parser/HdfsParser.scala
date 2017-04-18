package com.cpc.spark.qukan.parser

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

case class UserProfile(ok: Boolean, devid: String, age: Int, sex: Int, coin: Int)

/**
  * Created by Roy on 2017/4/14.
  */
object HdfsParser {

  val columnSep = '\001'

  def parseTextRow(txt: String): UserProfile = {
    val data = txt.split(columnSep)
    var profile = UserProfile(false, "", 0, 0, 0)
    if (data.length == 6) {
      val devid = data(0).trim
      if (devid.length > 0) {
        profile = UserProfile(
          ok = true,
          devid = devid,
          coin = toInt(data(2)),
          sex = toInt(data(3)),
          age = getAge(data(4))
        )
      }
    }
    profile
  }

  //年龄 0: 未知 1: 小于18 2:18-23 3:24-30 4:31-40 5:41-50 6: >50
  def getAge(birth: String): Int = {
    var year = 0
    if (birth.length == 10) {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val cal = Calendar.getInstance()
      try {
        cal.setTime(dateFormat.parse(birth))
        val byear = new SimpleDateFormat("yyyy").format(cal.getTime)
        val nyear = new SimpleDateFormat("yyyy").format(Calendar.getInstance().getTime)
        year = nyear.toInt - byear.toInt
      } catch {
        case e: Exception =>
          year = 0
      }
    }

    if (year < 1) {
      0
    } else if (year < 18) {
      1
    } else if (year < 23) {
      2
    } else if (year < 30) {
      3
    } else if (year < 40) {
      4
    } else if (year < 50) {
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


