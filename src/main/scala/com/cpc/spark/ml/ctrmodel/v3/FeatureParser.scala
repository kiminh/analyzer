package com.cpc.spark.ml.ctrmodel.v3

import java.util.Calendar
import mlserver.mlserver._
import com.cpc.spark.log.parser.UnionLog
import com.cpc.spark.ml.common.Dict
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by zhaolei on 2017/11/28.
  */
object FeatureParser {

  def unionLogToObject(x: UnionLog): (AdInfo, Media, AdSlot, User, Location, Network, Device, Long) = {
    var cls = 0
    var pagenum = 0
    var bookid = ""

    if (x.ext != null) {
      val ac = x.ext.getOrElse("adclass", null)
      if (ac != null) {
        cls = ac.int_value
      }

      val pn = x.ext.getOrElse("pagenum", null)
      if (pn != null) {
        pagenum = pn.int_value
      }

      val bi = x.ext.getOrElse("bookid", null)
      if (bi != null) {
        bookid = bi.string_value
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

    val ast = AdSlot(
      pageNum = pagenum,
      bookId = bookid
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
    (ad, m, ast, u, loc, n, d, x.timestamp * 1000L)
  }

  def parseUnionLog(x: UnionLog, dict: Dict): String = {
    val (ad, m, ast, u, loc, n, d, t) = unionLogToObject(x)
    var svm = ""
    val vector = getVector(dict, ad, m, ast, u, loc, n, d, x.timestamp * 1000L)
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

  def vectorToSvm(v: Vector): String = {
    var p = -1
    var svm = ""
    MLUtils.appendBias(v).foreachActive {
      (i, v) =>
        if (i <= p) {
          throw new Exception("svm error:" + v)
        }
        p = i
        svm = svm + " %d:%.0f".format(i + 1, v)
    }
    svm
  }

  def getVector(dict: Dict, ad: AdInfo, m: Media, ast: AdSlot, u: User,
                loc: Location, n: Network, d: Device, timeMills: Long): Vector = {

    val bookIdMap = Map[String,Int]("1" -> 1, "2" -> 2, "3" -> 3, "4" -> 4, "5" -> 5,
                                    "6" -> 6, "7" -> 7, "8" -> 8, "9" -> 9, "10" -> 10)

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

    //pagenum
    var pnum = 0
    if (ast.pageNum >= 1 && ast.pageNum <= 50){
      pnum = ast.pageNum
    }
    els = els :+ (pnum + 1 + i, 1d)
    i += 60

    //bookid
    els = els :+ (bookIdMap.getOrElse(ast.bookId,0) + i, 1d)
    i += 20

    try {
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }
}


