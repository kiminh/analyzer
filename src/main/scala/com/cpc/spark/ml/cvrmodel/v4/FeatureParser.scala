package com.cpc.spark.ml.cvrmodel.v4

import java.util.Calendar

import com.cpc.spark.log.parser.{ExtValue, TraceLog, UnionLog}
import com.cpc.spark.ml.common.{Dict, FeatureDict, Utils}
import mlserver.mlserver._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by Roy on 2017/5/15.
  */
object FeatureParser {

  def cvrPositive(traces: TraceLog*): Boolean = {
    var stay = 0
    var click = 0
    var active = 0
    traces.foreach {
      t =>
        t.trace_type match {
          case s if s.startsWith("active") => active += 1

          case "buttonClick" => click += 1

          //case "clickMonitor" => click += 1

          case "inputFocus" => click += 1

          case "press" => click += 1

          case "stay" =>
            if (t.duration > stay) {
              stay = t.duration
            }

          case _ =>
        }

    }

    (stay >= 30 && click > 0) || active > 0
  }


  def unionLogToObject(x: UnionLog, traces: TraceLog*): (AdInfo, Media, User, Location, Network, Device, Long) = {
    var cls = 0
    if (x.ext != null) {
      val v = x.ext.getOrElse("adclass", null)
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

    var chnl = 0
    if (x.ext != null) {
      val v = x.ext.getOrElse("channel", null)
      if (v != null) {
        if (v.string_value.length > 0) {
          chnl = v.string_value.toInt
        }
      }
    }
    val m = Media(
      mediaAppsid = x.media_appsid.toInt,
      mediaType = x.media_type,
      adslotid = x.adslotid.toInt,
      adslotType = x.adslot_type,
      floorbid = x.floorbid,
      channel = chnl
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
    var pl = 0
    if (x.ext != null) {
      val v = x.ext.getOrElse("phone_level", null)
      if (v != null) {
        pl = v.int_value
      }
    }
    val d = Device(
      os = x.os,
      model = x.model,
      phoneLevel = pl
    )

    (ad, m, u, loc, n, d, x.timestamp * 1000L)
  }

  def parseUnionLog(dict: Dict, x: UnionLog, traces: TraceLog*): String = {
    val (ad, m, u, loc, n, d, t) = unionLogToObject(x, traces:_*)
    var svm = ""
    val vector = getVector(dict, ad, m, u, loc, n, d, x.timestamp * 1000L)
    if (vector != null) {
      var p = -1
      if (cvrPositive(traces:_*)) {
        svm = "1"
      } else {
        svm = "0"
      }
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

  def parseUnionLogDNN(dict: Dict, x: UnionLog, traces: TraceLog*): String = {
    var svm = ""
    val vector = unionLogToDNNFeatures(dict, x, traces:_*)
    if (vector != null) {
      var p = -1
      if (cvrPositive(traces:_*)) {
        svm = "1"
      } else {
        svm = "0"
      }
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

  def unionLogToDNNFeatures(dict: Dict, x: UnionLog, traces: TraceLog*): Vector = {
    val (ad, m, u, loc, n, d, t) = unionLogToObject(x, traces:_*)
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(t)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)

    //age
    var age = 0
    if (u.age <= 1) {
      age = 1
    } else if (u.age <= 4) {
      age = 2
    } else {
      age = 3
    }

    val city = dict.city.getOrElse(loc.city, 0)
    val slotid = dict.adslot.getOrElse(m.adslotid, 0)
    val adcls = dict.adclass.getOrElse(ad._class, 0)
    val adtype = ad.adtype
    val mchannel = dict.channel.getOrElse(m.channel, 0)
    val adid = ad.ideaid % 9000

    var values: Array[Double] = Array(week, hour, u.sex, age, d.os, n.isp,
      n.network, city, slotid, adcls, adtype, mchannel, d.phoneLevel, adid)

    Vectors.dense(values)
  }

  def getVector(dict: Dict, ad: AdInfo, m: Media, u: User, loc: Location, n: Network,
            d: Device, timeMills: Long): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(timeMills)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[(Int, Double)]()
    var i = 0

    els = els :+ (week + i - 1, 1d)
    i += 7

    //(24)
    els = els :+ (hour + i, 1d)
    i += 24

    //interests
    u.interests.map(dict.interest.getOrElse(_, 0))
      .filter(_ > 0)
      .sortWith(_ < _)
      .foreach {
        intr =>
          els = els :+ (intr + i - 1, 1d)
      }
    i += 200

    els = els :+ (u.sex + i, 1d)
    i += 10

    //age
    var age = 0
    if (u.age <= 1) {
      age = 1
    } else if (u.age <= 4) {
      age = 2
    } else {
      age = 3
    }
    els = els :+ (age + i - 1, 1d)
    i += 100

    //os 96 - 97 (2)
    val os = d.os
    els = els :+ (os + i - 1, 1d)
    i += 10

    els = els :+ (n.isp + i, 1d)
    i += 19

    els = els :+ (n.network + i, 1d)
    i += 5

    val city = dict.city.getOrElse(loc.city, 0)
    els = els :+ (city + i, 1d)
    i += 1000

    //ad slot id
    val slotid = dict.adslot.getOrElse(m.adslotid, 0)
    els = els :+ (slotid + i, 1d)
    i += 1000

    //ad class
    val adcls = dict.adclass.getOrElse(ad._class, 0)
    els = els :+ (adcls + i, 1d)
    i += 1000

    val adtype = ad.adtype
    els = els :+ (adtype + i, 1d)
    i += 10

    val mchannel = dict.channel.getOrElse(m.channel, 0)
    els = els :+ (mchannel + i, 1d)
    i += 200

    //0 to 4
    els = els :+ (d.phoneLevel + i, 1d)
    i += 10

    //ideaid
    els = els :+ (i, ad.ideaid.toDouble)
    i += 1

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }
}

