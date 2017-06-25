package com.cpc.spark.ml.ctrmodel.v1

import java.util.Calendar

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.cpc.spark.ml.common.{FeatureDict, Utils}
import mlserver.mlserver._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by Roy on 2017/5/15.
  */
object FeatureParser extends FeatureDict {


  def parseUnionLog(x: UnionLog): String = {
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
      .filter(_._1 > 0)
      .sortWith((x, y) => x._2 > y._2)
      .filter(_._2.toInt >= 2)
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
      var p = -1;
      svm = x.isclick.toString
      MLUtils.appendBias(vector).foreachActive {
        (i, v) =>
          if (i <= p) {
            throw new Exception("svm error:" + vector.toString())
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

    els = els :+ (week - 1 + i, 1D)
    i += 7

    els = els :+ (hour + i, 1D)
    i += 24

    //interests
    u.interests.map(interests.getOrElse(_, 0))
      .filter(_ > 0)
      .sortWith(_ < _)
      .foreach {
        intr =>
          els = els :+ (intr + i, 1D)
      }
    i += interests.size

    //os
    var os = 0
    if (d.os > 0 && d.os < 3) {
      os = d.os
      els = els :+ (os + i, 1D)
    }
    i += 2

    //adslot type
    if (m.adslotType > 0) {
      els = els :+ (m.adslotType + i, 1D)
    }
    i += 2

    //ad type
    if (ad.adtype > 0) {
      els = els :+ (ad.adtype + i, 1D)
    }
    i += 6

    //ad class
    val adcls = adClass.getOrElse(ad._class, 0)
    if (adcls > 0) {
      els = els :+ (adcls + i, 1D)
    }
    i += adClass.size

    val city = cityDict.getOrElse(loc.city, 0)
    if (city > 0) {
      els = els :+ (city + i, 1D)
    }
    i += cityDict.size

    //userid
    els = els :+ (ad.userid + i, 1D)
    i += 2000

    //planid
    els = els :+ (ad.planid + i, 1D)
    i += 3000

    //unitid
    els = els :+ (ad.unitid + i, 1D)
    i += 10000

    //ideaid
    els = els :+ (ad.ideaid + i, 1D)
    i += 20000

    //ad slot id
    val slotid = adslotids.getOrElse(m.adslotid, 0)
    if (slotid > 0) {
      els = els :+ (slotid + i, 1D)
    }
    i += adslotids.size

    //adslotid + ideaid
    if (slotid > 0 && ad.ideaid > 0) {
      val v = Utils.combineIntFeatureIdx(slotid, ad.ideaid)
      els = els :+ (i + v, 1D)
    }
    i += adslotids.size * 20000

    //age
    var age = 0
    if (u.age <= 1) {
      age = 0
    } else if (age <= 4) {
      age = 1
    } else {
      age = 2
    }
    if (adcls > 0 && slotid > 0) {
      val v = Utils.combineIntFeatureIdx(u.sex + 1, age + 1, n.network + 1, adcls, slotid)
      els = els :+ (i + v, 1D)
    }
    i += 3 * 3 * 5 * adClass.size * adslotids.size

    if (u.sex > 0 && age > 0 && n.isp > 0 && adcls > 0 && slotid > 0) {
      val v = Utils.combineIntFeatureIdx(u.sex, age, n.isp, adcls, slotid)
      els = els :+ (i + v, 1D)
    }
    i += 2 * 2 * 18 * adClass.size * adslotids.size

    try {
      Vectors.sparse(i + 1, els)
    } catch {
      case e: Exception =>
        throw new Exception(e.getMessage + els.toString())
        null
    }
  }
}

