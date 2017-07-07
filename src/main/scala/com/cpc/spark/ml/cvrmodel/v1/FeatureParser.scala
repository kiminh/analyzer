package com.cpc.spark.ml.cvrmodel.v1

import java.util.Calendar

import com.cpc.spark.log.parser.{ExtValue, TraceLog, UnionLog}
import com.cpc.spark.ml.common.{FeatureDict, Utils}
import mlserver.mlserver._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by Roy on 2017/5/15.
  */
object FeatureParser extends FeatureDict {


  def parseUnionLog(x: UnionLog, traces: TraceLog*): String = {
    var cls = 0
    if (x.ext != null) {
      val v = x.ext.getOrElse("media_class", null)
      if (v != null) {
        cls = v.int_value
      }
    }
    val ad = AdInfo(
      bid = x.bid,
      ideaid = x.ideaid,
      unitid = x.unitid,
      planid = x.planid,
      userid = x.userid,
      adtype = x.adtype,
      interaction = x.interaction,
      _class = cls
    )
    val m = Media(
      mediaAppsid = x.media_appsid.toInt,
      mediaType = x.media_type,
      adslotid = x.adslotid.toInt,
      adslotType = x.adslot_type,
      floorbid = x.floorbid
    )
    val interests = x.interests.split(",")
      .map{
        x =>
          val v = x.split("=")
          if (v.length == 2) {
            (v(0).toInt, v(1).toInt)
          } else {
            (0, 0)
          }
      }
      .filter(x => x._1 > 0 && x._2 >= 2)
      .sortWith((x, y) => x._2 > y._2)
      .map(_._1)
      .toSeq
    val u = User(
      sex = x.sex,
      age = x.age,
      coin = x.coin,
      uid = x.uid,
      interests = interests
    )
    val n = Network(
      network = x.network,
      isp = x.isp,
      ip = x.ip
    )
    val loc = Location(
      country = x.country,
      province = x.province,
      city = x.city
    )
    val d = Device(
      os = x.os,
      model = x.model
    )

    var svm = ""
    val vector = parse(ad, m, u, loc, n, d, x.timestamp * 1000L)
    if (vector != null) {

      var stay = 0
      var click = 0
      var active = 0
      traces.foreach {
        t =>
          t.trace_type match {
            case s if s.startsWith("active") => active += 1

            case "buttonClick" => click += 1

            case "clickMonitor" => click += 1

            case "inputFocus" => click += 1

            case "press" => click += 1

            case "stay" =>
              if (t.duration > stay) {
                stay = t.duration
              }

            case _ =>
          }
      }

      if ((stay >= 30 && click > 0) || active > 0) {
        svm = "1"
      } else {
        svm = "0"
      }

      var p = -1
      MLUtils.appendBias(vector).foreachActive {
        (i, v) =>
          if (i <= p) {
            throw new Exception("svm error:" + vector)
          }
          p = i
          svm = svm + " %d:%f".format(i + 1, v)
      }
    }
    svm
  }

  def parse(ad: AdInfo, m: Media, u: User, loc: Location, n: Network, d: Device, timeMills: Long): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(timeMills)
    val week = cal.get(Calendar.DAY_OF_WEEK)
    val hour = cal.get(Calendar.HOUR_OF_DAY)

    var els = Seq[(Int, Double)]()
    var i = 0

    //0 - 6
    els = els :+ (week - 1 + i, 1d)
    i += 7

    //7 - 30 (24)
    els = els :+ (hour + i, 1d)
    i += 24

    //interests  31 - 95 (65)
    u.interests.map(interests.getOrElse(_, 0))
      .filter(_ > 0)
      .sortWith(_ < _)
      .foreach {
        intr =>
          els = els :+ (intr + i - 1, 1d)
      }
    i += interests.size

    //os 96 - 97 (2)
    val os = osDict.getOrElse(d.os, 0)
    if (d.os > 0) {
      els = els :+ (os + i - 1, 1d)
    }
    i += osDict.size

    //adslot type 98 - 99 (2)
    if (m.adslotType > 0) {
      els = els :+ (m.adslotType + i - 1, 1d)
    }
    i += 2

    //ad type  100 - 105 (6)
    if (ad.adtype > 0) {
      els = els :+ (ad.adtype + i - 1, 1d)
    }
    i += 6

    //userid  106 - 2105 (2000)
    if (ad.userid <= 2000) {
      els = els :+ (ad.userid + i - 1, 1d)
    }
    i += 2000

    //planid  2106 - 5105 (3000)
    if (ad.planid <= 3000) {
      els = els :+ (ad.planid + i - 1, 1d)
    }
    i += 3000

    //unitid  5106 - 15105 (10000)
    if (ad.unitid <= 10000) {
      els = els :+ (ad.unitid + i - 1, 1d)
    }
    i += 10000

    //ideaid  15106 - 35105 (20000)
    if (ad.ideaid <= 20000) {
      els = els :+ (ad.ideaid + i - 1, 1d)
    }
    i += 20000

    //ad slot id 35106 - 35152 (47)
    val slotid = adslotids.getOrElse(m.adslotid, 0)
    if (slotid > 0) {
      els = els :+ (slotid + i - 1, 1d)
    }
    i += adslotids.size

    //adslotid + ideaid  35153 - 975152 (940000)
    if (slotid > 0 && ad.ideaid > 0 && ad.ideaid <= 20000) {
      val v = Utils.combineIntFeatureIdx(slotid, ad.ideaid)
      els = els :+ (i + v - 1, 1d)
    }
    i += adslotids.size * 20000

    //age
    var age = 0
    if (u.age <= 1) {
      age = 0
    } else if (u.age <= 4) {
      age = 1
    } else {
      age = 2
    }
    //ad class
    val adcls = adClass.getOrElse(ad._class, 0)

    //975153 - 1594847 (619695)
    if (adcls > 0 && slotid > 0) {
      val v = Utils.combineIntFeatureIdx(u.sex + 1, age + 1, n.network + 1, adcls, slotid)
      els = els :+ (i + v - 1, 1d)
    }
    i += 3 * 3 * 5 * adClass.size * adslotids.size

    //1594848 - 2586359(991512)
    if (u.sex > 0 && age > 0 && n.isp > 0 && adcls > 0 && slotid > 0) {
      val v = Utils.combineIntFeatureIdx(u.sex, age, n.isp, adcls, slotid)
      els = els :+ (i + v - 1, 1d)
    }
    i += 2 * 2 * 18 * adClass.size * adslotids.size

    Vectors.sparse(i, els)
  }
}

