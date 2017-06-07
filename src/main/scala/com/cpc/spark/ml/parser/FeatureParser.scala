package com.cpc.spark.ml.parser

import java.util.Calendar

import com.cpc.spark.log.parser.UnionLog
import com.redis.RedisClient
import mlserver.mlserver._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import com.redis.serialization.Parse.Implicits._
import org.apache.spark.mllib.util.MLUtils

import scala.collection.mutable

/**
  * Created by Roy on 2017/5/15.
  */
object FeatureParser {

  def parseUnionLog(x: UnionLog, redis: RedisClient): String = {
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
    val vector = parse(ad, m, u, loc, n, d, x.timestamp * 1000L, redis)
    if (vector != null) {
      svm = x.isclick.toString
      vector.foreachActive {
        (i, v) =>
          svm = svm + " %d:%f".format(i, v)
      }
    }
    svm
  }

  def parse(ad: AdInfo, m: Media, u: User, loc: Location, n: Network, d: Device,
            timeMills: Long, redis: RedisClient): Vector = {

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
    els = els :+ (u.sex + i, 1D)
    i += 5

    //age
    els = els :+ (u.age + i, 1D)
    i += 10

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

    //coin
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

    //isp
    els = els :+ (n.isp + i, 1D)
    i += 50

    //net
    els = els :+ (n.network + i, 1D)
    i += 10

    //city 0 - 1000
    els = els :+ (loc.city + i, 1D)
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
    i += 5000

    //ideaid
    els = els :+ (ad.ideaid + i, 1D)
    i += 20000

    //ad slot id
    els = els :+ (m.adslotid % 10000 + i, 1D)
    i += 10000

    //model
    if (d.model.length > 0) {
      els = els :+ (d.model.hashCode % 1000 + 1000 + i, 1D)
    }
    i += 2000

    els = els :+ (i, m.floorbid.toDouble)
    i += 1

    els = els :+ (i, ad.bid.toDouble)
    i += 1

    if (u.uid.length > 0) {
      var clk = redis.get[Int]("MLFeatureCtr-uid-clk-" + u.uid).getOrElse(0)
      if (clk > 5) {
        clk = 5
      }
      els = els :+ (clk + i, 1D)
    }
    i += 6

    if (u.uid.length > 0) {
      var pv = redis.get[Int]("MLFeatureCtr-uid-pv-" + u.uid).getOrElse(0)
      if (pv <= 0) {
        pv = 0
      } else if (pv < 10) {
        pv = 1
      } else if (pv < 100) {
        pv = 2
      } else if (pv < 500) {
        pv = 3
      } else {
        pv = 4
      }
      els = els :+ (pv + i, 1D)
    }
    i += 5

    try {
      MLUtils.appendBias(Vectors.sparse(i, els))
    } catch {
      case e: Exception =>
        println(els)
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

  def vectorizeIntRange(max: Int, v: Int): Vector = {
    Vectors.sparse(max, Seq((v, 1D)))
  }
}

