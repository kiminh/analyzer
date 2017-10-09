package com.cpc.spark.ml.antimodel.v2

import java.util.Calendar

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.cpc.spark.ml.antimodel.v2.CreateSvm.Antispam
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
// (uid,request,show,click,ip,ctr,isAtispam ,load, active, buttonClick, press, stay1, stay5, stay10, stay30, stay60, stay120)

  def parseLog(log:Antispam): String = {
    val x = log
    var svm = log.uid + ","
    val vector = getVector(log)
    if (vector != null) {
      var p = -1
      svm = svm + log.isAtispam.toString
      MLUtils.appendBias(vector).foreachActive {
        (i, v) =>
          if (i <= p) {
            throw new Exception("svm error:" + vector)
          }
          p = i
          svm = svm + " %d:%f".format(i + 1, v)
      }
    }else{
      svm = ""
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

  def getVector(log:Antispam): Vector = {
    var els = Seq[(Int, Double)]()
    var i = 0
    try {

      els = els :+ ( getRequest(log.request) + i, 1d)
      i += 30

      els = els :+ ( getRequest(log.show) + i, 1d)
      i += 30

      var click = log.click % 1000
      if(log.click>= 1000){
        click =1000
      }
      els = els :+ (click + i, 1d)
      i += 1000

      var ctr = log.ctr % 100
      els = els :+ (ctr + i, 1d)
      i += 100

      var ipNum = log.ipNum % 200
      els = els :+ (ipNum + i, 1d)
      i += 200

      var load = log.load % 200
      els = els :+ (load + i, 1d)
      i += 200
      var active = log.active % 200
      els = els :+ (active + i, 1d)
      i += 200
      var stay1 = log.stay1 % 200
      els = els :+ (stay1 + i, 1d)
      i += 200

      var stay5 = log.stay5 % 200
      els = els :+ (stay5 + i, 1d)
      i += 200
      var stay10 = log.stay10 % 200
      els = els :+ (stay10 + i, 1d)
      i += 200
      var stay30 = log.stay30 % 200
      els = els :+ (stay30 + i, 1d)
      i += 200
      var stay60 = log.stay60 %200
      els = els :+ (stay60 + i, 1d)
      i += 200
      var stay120 = log.stay120 % 200
      els = els :+ (stay120 + i, 1d)
      i += 200

      var coin = getCoinTag(log.coin) % 22
      els = els :+ (coin + i, 1d)
      i += 22
      var contentNum = log.contentNum % 1000
      els = els :+ (contentNum + i, 1d)
      i += 1000
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage)
        null
    }
  }
  def getRequest(y:Int):Int ={
    if(y<0){
      1
    }else if (y == 0){
      2
    }else if (y<=10){
      3
    }else if (y <= 20){
      4
    }else if (y <= 30){
      5
    }else if (y <= 40){
      6
    }else if (y <= 50){
      7
    }else if (y <= 60){
      8
    }else if (y <= 70){
      9
    }else if (y <= 80){
      10
    }else if (y <= 90){
      11
    }else if (y<= 100) {
      12
    } else if (y<= 200) {
      13
    } else if (y<= 300) {
      14
    } else if (y<= 400) {
      15
    }else if (y <= 500){
      16
    } else if (y<=600) {
      17
    } else if (y<=700) {
      18
    } else if (y<=800) {
      19
    } else if (y<=900) {
      20
    } else if (y <= 1000){
      21
    } else if (y <= 1500){
      22
    } else if (y <= 1700){
      23
    } else if (y <= 1900){
      24
    }else if (y <= 2000){
      25
    }else if (y <= 3000){
      26
    }else if (y <= 4000){
      27
    }else if (y <= 5000){
      28
    }else if (y <=6000){
      29
    }else{
      30
    }
  }
  def getCoinTag(y: Int):Int ={
    if(y<0){
       1
    }else if (y == 0){
       2
    }else if (y<=10){
       3
    }else if (y <= 20){
       4
    }else if (y <= 30){
       5
    }else if (y <= 40){
       6
    }else if (y <= 50){
      7
    }else if (y <= 60){
       8
    }else if (y <= 70){
      9
    }else if (y <= 80){
      10
    }else if (y <= 90){
       11
    }else if (y<= 100) {
       12
    } else if (y<= 200) {
       13
    } else if (y<= 300) {
       14
    } else if (y<= 400) {
       15
    }else if (y <= 500){
       16
    } else if (y<=1000) {
       17
    } else if (y<=2000) {
       18
    } else if (y<=5000) {
       19
    } else if (y<=10000) {
       20
    }else {
       21
    }
  }
}

