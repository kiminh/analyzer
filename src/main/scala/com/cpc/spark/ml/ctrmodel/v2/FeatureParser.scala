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

  def parse(ad: AdInfo, m: Media, u: User, loc: Location, n: Network, d: Device, timeMills: Long): Vector = {
    var els = Seq[(Int, Double)]()
    var i = 0

    //interests   (65)
    u.interests.map(interests.getOrElse(_, 0))
      .filter(_ > 0)
      .sortWith(_ < _)
      .foreach {
        intr =>
          els = els :+ (intr + i - 1, 1d)
      }
    i += interests.size

    val slotid = adslotids.getOrElse(m.adslotid, 0)
    //adslotid + ideaid   (940000)
    if (slotid > 0 && ad.ideaid > 0 && ad.ideaid <= 20000) {
      val v = Utils.combineIntFeatureIdx(slotid, ad.ideaid)
      els = els :+ (i + v - 1, 1d)
    }
    i += adslotids.size * 20000

    //age
    var age = 0
    if (u.age <= 1) {
      age = 1
    } else if (u.age <= 4) {
      age = 2
    } else {
      age = 3
    }
    //ad class
    val adcls = adClass.getOrElse(ad._class, 0)
    var isp = 0
    if (n.isp > 0 && n.isp < 4) {
      isp = n.isp
    } else if (n.isp == 9) {
      isp = 4
    } else {
      isp = 5
    }
    /*
    组合特征
    sex age network isp adcls slotid
     3   3    5      5   293   47    3098475
     */
    if (adcls > 0 && slotid > 0) {
      val v = Utils.combineIntFeatureIdx(u.sex + 1, age, n.network + 1, isp, adcls, slotid)
      els = els :+ (i + v - 1, 1d)
    }
    i += 3 * 3 * 5 * 5 * adClass.size * adslotids.size

    Vectors.sparse(i, els)
  }
}

