package com.cpc.spark.ml.antimodel.v1

import java.util.Calendar

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.cpc.spark.ml.antimodel.v1.CreateSvm.TLog
import com.cpc.spark.ml.common.Dict
import mlserver.mlserver._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by Roy on 2017/5/15.
  */
object FeatureParser {

  def unionLogToObject(x: UnionLog): (AdInfo, Media, User, Location, Network, Device, Long) = {
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
    (ad, m, u, loc, n, d, x.timestamp * 1000L)
  }

  def parseUnionLog(y:(String, ((UnionLog, Option[Int]), UserInfo)), dict: Dict): String = {
    val x = y._2._1._1
    val (ad, m, u, loc, n, d, t) = unionLogToObject(x)
    var svm = "0"
    val vector = getVector(dict, ad, m, u, loc, n, d, x.timestamp * 1000L, y._2._2, y._2._1._2)
    if (vector != null) {
      var p = -1
      svm = x.ext.getOrElse("antispam", ExtValue()).int_value.toString
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
        svm = svm + " %d:%f".format(i + 1, v)
    }
    svm
  }

  def getVector(dict: Dict, ad: AdInfo, m: Media, u: User,
            loc: Location, n: Network, d: Device, timeMills: Long, user:UserInfo, trace: Option[Int]): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(timeMills)
    val week = cal.get(Calendar.DAY_OF_WEEK)   //1 to 7
    val hour = cal.get(Calendar.HOUR_OF_DAY)
    var els = Seq[(Int, Double)]()
    var i = 0
    try {
      els = els :+ (week + i - 1, 1d)
      i += 7

      //(24)
      els = els :+ (hour + i, 1d)
      i += 24

      //interests
      /*u.interests.map(dict.interest.getOrElse(_, 0))
        .filter(_ > 0)
        .sortWith(_ < _)
        .foreach {
          intr =>
            els = els :+ (intr + i - 1, 1d)
        }
      i += 200*/

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

      var city = dict.city.getOrElse(loc.city, 0)
      city = city % 1000
      els = els :+ (city + i, 1d)
      i += 1000

      //ad slot id
      var slotid = dict.adslot.getOrElse(m.adslotid, 0)
      slotid = slotid % 1000
      els = els :+ (slotid + i, 1d)
      i += 1000

      //ad class
      var adcls = dict.adclass.getOrElse(ad._class, 0)
      adcls = adcls % 1000
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
    /*  var adid = 0
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
      i += 200000*/
     /* user.foreach{
        x =>
          var key = 0
          key = x.coinType + x.coin
          if(key >= 1000){
            key = 0
          }
          els = els :+ (key + i, 1d)
          i += 1000
      }
      user.foreach{
        x =>
          var key = 0
          key = x.cmdType + x.contentNum
          if(key >= 1000){
            key = 0
          }
          els = els :+ (key + i, 1d)
          i += 1000
      }*/
      var coin = 0
      if(user.coin >= 20000){
         coin = 0
      }else{
        coin =  user.coin
      }
      els = els :+ (coin + i, 1d)
      i += 20000

     var  contentNum = user.contentNum
      if(contentNum >= 1000){
        contentNum = 1
      }
      els = els :+ (contentNum + i, 1d)
      i += 1000

      var duration = trace.getOrElse(0)
      if(duration >= 100){
        duration = 1
      }
      els = els :+ (duration + i, 1d)
      i += 100
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }

  }

}

