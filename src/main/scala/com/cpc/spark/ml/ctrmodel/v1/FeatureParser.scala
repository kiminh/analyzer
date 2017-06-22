package com.cpc.spark.ml.ctrmodel.v1

import java.util.Calendar

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.cpc.spark.ml.common.FeatureDict
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
      .filter(_._2.toInt >= 4)
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
      svm = x.isclick.toString
      MLUtils.appendBias(vector).foreachActive {
        (i, v) =>
          svm = svm + " %d:%f".format(i, v)
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

    els = els :+ (week, 1D)
    i += 7

    els = els :+ (hour + i, 1D)
    i += 24

    //sex
    if (u.sex > 0) {
      els = els :+ (u.sex + i, 1D)
    }
    i += 2

    //interests
    val mostInterest = 0
    u.interests.foreach {
      intr =>
        els = els :+ (interests.getOrElse(intr, 0) + i, 1D)
    }
    i += interests.size

    //coin
    var lvl = 0
    if (u.coin < 10) {
      lvl = 1
    } else if (u.coin < 1000) {
      lvl = 2
    } else if (u.coin < 10000) {
      lvl = 3
    } else {
      lvl = 4
    }
    els = els :+ (lvl + i, 1D)
    i += 10

    //os
    els = els :+ (d.os + i, 1D)
    i += 10

    //adslot type
    els = els :+ (m.adslotType + i, 1D)
    i += 10

    //ad type
    els = els :+ (ad.adtype + i, 1D)
    i += 10

    //interaction
    els = els :+ (ad.interaction + i, 1D)
    i += 10

    //ad class
    val adcls = adClass.getOrElse(ad._class, 0)
    if (adcls > 0) {
      els = els :+ (adcls + i, 1D)
    }
    i += adClass.size

    //isp
    els = els :+ (n.isp + i, 1D)
    i += 50

    //net
    els = els :+ (n.network + i, 1D)
    i += 10

    //city 0 - 1000
    val city = loc.city % 1000
    els = els :+ (city + i, 1D)
    i += 1000

    //media id
    els = els :+ (m.mediaAppsid % 100 + i, 1D)
    i += 100

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

    //model
    if (d.model.length > 0) {
      els = els :+ (d.model.hashCode % 1000 + 1000 + i, 1D)
    }
    i += 2000

    //sex adclass slot
    val max1 = 2 * adClass.size * adslotids.size
    if (u.sex > 0 && adcls > 0 && slotid > 0) {
      val v = (u.sex - 1) + (adcls - 1) * u.sex + (slotid - 1) * adcls * u.sex
      els = els :+ (i + v, 1D)
    }
    i += max1

    //sex interest adclass slot
    val interest = interests.getOrElse(u.interests.headOption.getOrElse(0), 0)
    val max2 = 2 * interests.size * adClass.size * adslotids.size
    if (u.sex > 0 && interest > 0 && adcls > 0 && slotid > 0) {
      val v = (u.sex - 1) + (adcls - 1) * u.sex + (slotid - 1) * adcls * u.sex +
        (interest - 1) * adcls * u.sex * adcls * u.sex
      els = els :+ (i + v, 1D)
    }
    i += max2

    //sex net adclass slot
    val max3 = 2 * 4 * adClass.size * adslotids.size
    if (u.sex > 0 && n.network > 0 && adcls > 0 && slotid > 0) {
      val v = (u.sex - 1) + (adcls - 1) * u.sex + (slotid - 1) * adcls * u.sex +
        (n.network - 1) * adcls * u.sex * adcls * u.sex
      els = els :+ (i + v, 1D)
    }
    i += max3

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        println(e.getMessage, els)
        null
    }
  }

  /*
  返回组合特征的位置，和最大位置号
   */
  def combineIntFeature(min1: Int, max1: Int, v1: Int, min2: Int, max2: Int, v2: Int): (Int, Int) = {
    val range1 = max1 - min1 + 1
    val range2 = max2 - min2 + 1
    val idx1 = v1 - min1 + 1
    val idx2 = v2 - min2 + 1
    (range2 * (idx1 - 1) + idx2, range1 * range2)
  }
}

