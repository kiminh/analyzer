package com.cpc.spark.ml.cvrmodel.v1

import java.util.Calendar

import com.cpc.spark.log.parser.{ExtValue, TraceLog, UnionLog}
import com.cpc.spark.ml.common.{Dict, FeatureDict, Utils}
import com.cpc.spark.ml.cvrmodel.v1.CreateSvm.TLog
import mlserver.mlserver._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by Roy on 2017/5/15.
  */
object FeatureParser {

  def cvrPositive(traces: TLog*): Boolean = {
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


  def parseUnionLog(dict: Dict, x: UnionLog, traces: TLog*): String = {
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

    var svm = ""
    val vector = parse(dict, ad, m, u, loc, n, d, x.timestamp * 1000L)
    if (vector != null) {

      if (cvrPositive(traces:_*)) {
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

  def parse(dict: Dict, ad: AdInfo, m: Media, u: User, loc: Location, n: Network, d: Device, timeMills: Long): Vector = {
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

    //ideaid  (200000)
    var adid = 0
    if (ad.ideaid >= 1500000) {
      adid = ad.ideaid - 1500000 + 60000   //新平台
    } else if (ad.ideaid >= 1000000) {
      adid = ad.ideaid - 1000000 + 30000   //迁移到新平台
    } else if (ad.ideaid < 30000) {
      adid = ad.ideaid  //老平台
    }
    if (adid >= 200000) {
      adid = 0
    }
    els = els :+ (adid + i, 1d)
    i += 200000

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }
}

