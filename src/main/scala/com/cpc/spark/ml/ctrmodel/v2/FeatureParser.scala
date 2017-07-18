package com.cpc.spark.ml.ctrmodel.v2

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
    val vector = parse(ad, m, u, loc, n, d, x.timestamp * 1000L)
    if (vector != null) {
      var p = -1
      svm = x.isclick.toString
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

  def parse(ad: AdInfo, m: Media, u: User, loc: Location, n: Network,
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

    //interests   (65)
    u.interests.map(interests.getOrElse(_, 0))
      .filter(_ > 0)
      .sortWith(_ < _)
      .foreach {
        intr =>
          els = els :+ (intr + i - 1, 1d)
      }
    i += interests.size

    els = els :+ (u.sex + i, 1d)
    i += 3

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
    i += 3

    //os 96 - 97 (2)
    val os = osDict.getOrElse(d.os, 0)
    if (d.os > 0) {
      els = els :+ (os + i - 1, 1d)
    }
    i += osDict.size

    els = els :+ (n.isp + i, 1d)
    i += 19

    els = els :+ (n.network + i, 1d)
    i += 5

    val city = cityDict.getOrElse(loc.city, 0)
    els = els :+ (city + i, 1d)
    i += cityDict.size + 1

    //ideaid  15106 - 35105 (20000)
    if (ad.ideaid <= 20000) {
      els = els :+ (ad.ideaid + i - 1, 1d)
    }
    i += 20000

    /*
    //ideaid  (60000)
    var adid = 0
    if (ad.ideaid >= 1500000) {
      adid = ad.ideaid - 1500000 + 40000
    } else if (ad.ideaid >= 1000000) {
      adid = ad.ideaid - 1000000 + 20000
    } else {
      adid = ad.ideaid
    }
    if (adid >= 60000) {
      adid = 0
    }
    els = els :+ (adid + i, 1d)
    i += 60000
    */

    //ad slot id 35106 - 35152 (47)
    val slotid = adslotids.getOrElse(m.adslotid, 0)
    els = els :+ (slotid + i, 1d)
    i += adslotids.size + 1

    //ad class
    val adcls = adClass.getOrElse(ad._class, 0)
    els = els :+ (adcls + i, 1d)
    i += adClass.size + 1

    val mchannel = MediaChannelDict.getOrElse(m.channel, 0)
    els = els :+ (mchannel + i, 1d)
    i += MediaChannelDict.size + 1

    //0 to 4
    els = els :+ (d.phoneLevel + i, 1d)
    i += 5

    /*
    //slotid city adcls channel network isp sex phone_level
    val comFeatures = Seq[(Int, Int)](
      (slotid + 1, adslotids.size + 1),
      (city + 1, cityDict.size + 1),
      (adcls + 1, adClass.size + 1),
      (mchannel + 1, MediaChannelDict.size + 1),
      (n.network + 1, 5),
      (n.isp + 1, 19),
      (u.sex + 1, 3),
      (d.phoneLevel + 1, 5)
    )

    //所有2个特征组合
    var maxSize = 0
    Utils.getCombination(comFeatures, 2)
      .foreach {
        combine =>
          val x = combine(0)
          val y = combine(1)
          val v = Utils.combineIntFeatureIdx(x._1, y._1)

          els = els :+ (v + i, 1d)
          i += x._2 * y._2
      }
      */

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }
}

