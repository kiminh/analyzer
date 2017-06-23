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

    //interests
    u.interests.foreach {
      intr =>
        els = els :+ (interests.getOrElse(intr, 0) + i, 1D)
    }
    i += interests.size

    //isp
    if (n.isp > 0) {
      els = els :+ (n.isp + i, 1D)
    }
    i += 18

    //ad slot id
    val os = osDict.getOrElse(d.os, 0)
    val adcls = adClass.getOrElse(ad._class, 0)
    val city = cityDict.getOrElse(loc.city, 0)
    val slotid = adslotids.getOrElse(m.adslotid, 0)

    //sex 3 os 3 net 5 city 433 adclass 294 adtype 7 slotid 48
    val v = Utils.combineIntFeatureIdx(u.sex + 1, os + 1, n.network + 1, city + 1, adcls + 1, ad.adtype + 1, slotid + 1)
    els = els :+ (i + v, 1D)
    i += 3 * 3 * 5 * 433 * 294 * 7 * 48

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        println(e.getMessage, els)
        null
    }
  }
}

