package com.cpc.spark.qukan.parser

import java.text.SimpleDateFormat
import java.util.Calendar

import com.typesafe.config.Config
import org.apache.spark.sql.Row
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable


/**
  * Created by Roy on 2017/4/14.
  */
object HdfsParser {

  val columnSep = '\001'

  val columnSepTab = '\t'

  def parseTextRow(txt: String): ProfileRow = {
    val data = txt.split(columnSep)
    var profile: ProfileRow = null
    if (data.length == 6) {
      val devid = data(0).trim
      if (devid.length > 0) {
        profile = ProfileRow(
          devid = devid,
          age = getAge(data(4)),
          sex = toInt(data(3)),
          coin = toInt(data(2)),
          from = 0
        )
      }
    }
    profile
  }


  def parseTextRowAgeSex(row: Row): ProfileRow = {
    var profile: ProfileRow = null
    if (row.length == 4) {
      val devid = row.getString(0).trim
      if (devid.length > 0) {
        val sex = row.getString(2).trim match {
          case "2500100010" => 1
          case "2500100020" => 2
          case "2500100030" => 0
          case _ => 0
        }

        /*
        0-13 2600100000
        14-17 2600100001
        18-19 2600100002
        20-24 2600100003
        25-29 2600100004
        30-34 2600100005
        35-39 2600100006
        40-44 2600100007
        45-49 2600100008
        50+   2600100009
        未知   260010001
        */
        val age = row.getString(3).trim match {
          case "2600100000" => 1
          case "2600100001" => 1
          case "2600100002" => 2
          case "2600100003" => 2
          case "2600100004" => 3
          case "2600100005" => 4
          case "2600100006" => 4
          case "2600100007" => 5
          case "2600100008" => 5
          case "2600100009" => 6
          case "260010001" => 0
          case _ => 0
        }

        profile = ProfileRow(
          devid = devid,
          age = age,
          sex = sex
        )
      }
    }
    profile
  }

  def parseInstallApp(x: Row, f: String => Boolean, pkgTags: Config): ProfileRow = {
    var profile: ProfileRow = null
    val devid = x.getString(1)
    if (devid != null && devid.length > 0) {
      val data = x.getString(2)
      val dataType = x.getInt(5)
      try {
        var pkgs: List[AppPkg] = null

        //data 有2种格式的数据
        if (dataType == 1) {
          pkgs = for {
            JArray(pkgs) <- parse(x.getString(2))
            JObject(pkg) <- pkgs
            JField("firstInstallTime", JInt(ftime)) <- pkg
            JField("lastUpdateTime", JInt(ltime)) <- pkg
            JField("packageName", JString(pname)) <- pkg
            p = AppPkg(
              name = pname,
              firstInstallTime = ftime.toLong,
              lastUpdateTime = ltime.toLong
            )
          } yield p
        } else {
          pkgs = for {
            JArray(pkgs) <- parse(x.getString(2))
            JString(pname) <- pkgs
            p = AppPkg(
              name = pname
            )
          } yield p
        }

        if (pkgs.length > 0) {
          var uis = List[UserInterest]()
          if (pkgTags != null) {
            val utags = mutable.Map[Int, Int]()
            pkgs.foreach {
              p =>
                val key = p.name.replace('.', '|')
                if (pkgTags.hasPath(key)) {
                  val tags = pkgTags.getIntList(key)
                  for (i <- 0 to tags.size() - 1) {
                    val tag = tags.get(i).toInt
                    val v = utags.getOrElseUpdate(tag, 0)
                    utags.update(tag, v + 1)
                  }
                }
            }

            for ((k, v) <- utags) {
              uis = uis :+ UserInterest(tag = k, score = v)
            }
          }

          profile = ProfileRow(
            devid = devid,
            from = 1,
            pkgs = pkgs.filter(x => f(x.name)),
            uis = uis
          )
        }
      } catch {
        case e: Exception => null
      }
    }
    profile
  }

  //年龄 0: 未知 1: 小于18 2:18-23 3:24-30 4:31-40 5:41-50 6: >50
  def getAge(birth: String): Int = {
    var year = 0
    if (birth.length == 10) {
      val cal = Calendar.getInstance()
      try {
        cal.setTime(new SimpleDateFormat("yyyy-MM-dd").parse(birth))
        val yearFormat = new SimpleDateFormat("yyyy")
        val byear = yearFormat.format(cal.getTime)
        val nyear = yearFormat.format(Calendar.getInstance().getTime)
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


case class ProfileRow (
                      devid: String = "",
                      age: Int = 0,
                      sex: Int = 0,
                      coin: Int = 0,
                      pcate: Int = 0,
                      from: Int = 0,
                      pkgs: List[AppPkg] = List[AppPkg](),
                      uis: List[UserInterest] = List[UserInterest]()
                      )

case class AppPkg (
                  name: String = "",
                  firstInstallTime: Long = 0,
                  lastUpdateTime: Long = 0
                  )


case class UserInterest (
                        tag: Int = 0,
                        score: Int = 0
                        )


