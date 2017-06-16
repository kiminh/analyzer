package com.cpc.spark.ml.parser

import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import mlserver.mlserver._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils

import scala.collection.mutable

/**
  * Created by Roy on 2017/5/15.
  */
object FeatureParserV2 {

  val adslotids = Map(
    1010244 -> 1,
    1012765 -> 2,
    1012947 -> 3,
    1018976 -> 4,
    1020714 -> 5,
    1020995 -> 6,
    1021639 -> 7,
    1021642 -> 8,
    1022064 -> 9,
    1022280 -> 10,
    1022704 -> 11,
    1022709 -> 12,
    1022710 -> 13,
    1022798 -> 14,
    1023933 -> 15,
    1023934 -> 16,
    1023935 -> 17,
    1024360 -> 18,
    1024852 -> 19,
    1024902 -> 20,
    1025099 -> 21,
    1025156 -> 22,
    1025164 -> 23,
    1025493 -> 24,
    1026231 -> 25,
    1026459 -> 26,
    1026558 -> 27,
    1026890 -> 28,
    1026966 -> 29,
    1026975 -> 30,
    1027091 -> 31,
    1027156 -> 32,
    1008946 -> 33
  )

  def parseUnionLog(x: UnionLog, clk: Int, pv: Int): String = {
    val ad = AdInfo(
      bid = x.bid,
      ideaid = x.ideaid,
      unitid = x.unitid,
      planid = x.planid,
      userid = x.userid,
      adtype = x.adtype,
      interaction = x.interaction
    )
    val m = Media(
      mediaAppsid = x.media_appsid.toInt,
      mediaType = x.media_type,
      adslotid = x.adslotid.toInt,
      adslotType = x.adslot_type,
      floorbid = x.floorbid
    )
    val u = User(
      sex = x.sex,
      age = x.age,
      coin = x.coin,
      uid = x.uid
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

    var els = Seq[(Int, Double)]()
    var i = 0

    //sex
    els = els :+ (u.sex + i, 1D)
    i += 5

    //age
    els = els :+ (u.age + i, 1D)
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

    //media id
    els = els :+ (m.mediaAppsid % 100 + i, 1D)
    i += 100

    //planid
    els = els :+ (ad.planid + i, 1D)
    i += 3000

    //ideaid
    els = els :+ (ad.ideaid + i, 1D)
    i += 20000

    //ad slot id
    val slotid = adslotids.getOrElse(m.adslotid, 0)
    els = els :+ (slotid + i, 1D)
    i += adslotids.size

    //adslotid + ideaid
    val (v, max) = combineIntFeature(0, adslotids.size, adslotids.getOrElse(m.adslotid, 0), 0, 20000, ad.ideaid)
    els = els :+ (i + v, 1D)
    i += max

    //adslotid + planid
    val (v2, max2) = combineIntFeature(0, adslotids.size, adslotids.getOrElse(m.adslotid, 0), 0, 3000, ad.planid)
    els = els :+ (i + v2, 1D)
    i += max2
    //adslottype + ideaid
    val (v3, max3) = combineIntFeature(0, 2, m.adslotType, 0, 20000, ad.ideaid)
    els = els :+ (i + v3, 1D)
    i += max3

    //adslottype + ideaid
    val (v4, max4) = combineIntFeature(0, 2, m.adslotType, 0, 3000, ad.planid)
    els = els :+ (i + v4, 1D)
    i += max4

    val uid = u.uid.hashCode % 5000000 + 10000000
    els = els :+ (uid + i, 1D)
    i += 10000000

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        println(e.getMessage, els)
        null
    }
  }

  //得到所有排列组合 C(n, m)
  def getCombination[T: Manifest](all: Seq[T], m: Int): Seq[Array[T]] = {
    var combs = mutable.Seq[Array[T]]()
    val comb = new Array[T](m)
    def mapCombination(all: Seq[T], m: Int, start: Int, idx: Int, comb: Array[T]): Unit = {
      if (m > 0) {
        for (i <- start to all.length - 1) {
          comb(idx) = all(i)
          mapCombination(all, m - 1, i + 1, idx + 1, comb)
        }
      } else {
        combs :+= comb
      }
    }
    mapCombination(all, m, 0, 0, comb)
    combs
  }

  def normalize(min: Vector, max: Vector, row: Vector): Vector = {
    var els = Seq[(Int, Double)]()
    row.foreachActive {
      (i, v) =>
        var rate = 0.5D
        if (max(i) > min(i)) {
          if (v < min(i)) {
            rate = 1e-6
          } else if (v > max(i)) {
            rate = 1 - 1e-6
          } else {
            rate = (v - min(i)) / (max(i) - min(i))
          }
        }
        els = els :+ (i, rate)
    }
    Vectors.sparse(row.size, els)
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

