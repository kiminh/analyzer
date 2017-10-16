package com.cpc.spark.ml.antimodel.v1

import java.util.Calendar

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.cpc.spark.ml.antimodel.v1.CreateSvm.{TraceLog2}
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

  def parseUnionLog(unionLog:UnionLog): String = {
    var svm = "0"
    val vector = getVector(unionLog)
    if (vector != null) {
      var p = -1
      svm = unionLog.ext.getOrElse("antispam", ExtValue()).int_value.toString
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

  def getVector(unionLog:UnionLog): Vector = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(unionLog.timestamp)
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

      els = els :+ (unionLog.sex + i, 1d)
      i += 10

      //age
      var age = 0
      if (unionLog.age <= 1) {
        age = 1
      } else if (unionLog.age <= 4) {
        age = 2
      } else {
        age = 3
      }
      els = els :+ (age + i - 1, 1d)
      i += 100

      //os 96 - 97 (2)
      val os = unionLog.os
      els = els :+ (os + i - 1, 1d)
      i += 10

      els = els :+ (unionLog.isp + i, 1d)
      i += 19

      els = els :+ (unionLog.network + i, 1d)
      i += 5

      var city = unionLog.city % 1000
      els = els :+ (city + i, 1d)
      i += 1001

      //ad slot id
      var slotid = unionLog.adslotid.toInt % 1000
      els = els :+ (slotid + i, 1d)
      i += 1001

      //ad class
      var adcls = unionLog.ext.getOrElse("adclass",ExtValue()).int_value % 1000
      els = els :+ (adcls + i, 1d)
      i += 1001

      var coin = unionLog.ext.getOrElse("share_coin",ExtValue()).int_value
      if(coin >= 1000){
         coin = 999
      }else{
        coin =  coin
      }
      els = els :+ (coin + i, 1d)
      i += 1001
      els = els :+ (getTag(unionLog.screen_h)+ i, 1d)
      i += 34
      els = els :+ (getTag(unionLog.screen_w)+ i, 1d)
      i += 34

      /*var load = if(trace.load > 0 ) 1 else 0
      els = els :+ (load + i, 1d)
      i += 2
      var active = if(trace.active > 0 ) 1 else 0
      els = els :+ (active + i, 1d)
      i += 2

      var buttonClick = if(trace.buttonClick > 20 ) 20 else trace.buttonClick
      els = els :+ (buttonClick + i, 1d)
      i += 21

      var press = if(trace.press > 20 ) 20 else trace.press
      els = els :+ (press + i, 1d)
      i += 21
      var stay1 = if(trace.stay1 > 0 ) 1 else 0
      els = els :+ (stay1 + i, 1d)
      i += 2

      var stay5 = if(trace.stay5 > 0 ) 1 else 0
      els = els :+ (stay5 + i, 1d)
      i += 2

      var stay10 = if(trace.stay10 > 0 ) 1 else 0
      els = els :+ (stay10 + i, 1d)
      i += 2
      var stay30 = if(trace.stay30 > 0 ) 1 else 0
      els = els :+ (stay30 + i, 1d)
      i += 2

      var stay60 = if(trace.stay60 > 0 ) 1 else 0
      els = els :+ (stay60 + i, 1d)
      i += 2

      var stay120 = if(trace.stay120 > 0 ) 1 else 0
      els = els :+ (stay120 + i, 1d)
      i += 2

      els = els :+ (getTag(trace.screen_h)+ i, 1d)
      i += 34
      els = els :+ (getTag(trace.screen_w)+ i, 1d)
      i += 34
      els = els :+ (getTag(trace.client_h)+ i, 1d)
      i += 34
      els = els :+ (getTag(trace.client_w)+ i, 1d)
      i += 34
      els = els :+ (getTag(trace.client_x)+ i, 1d)
      i += 34
      els = els :+ (getTag(trace.page_x)+ i, 1d)
      i += 34
      els = els :+ (getTag(trace.page_y)+ i, 1d)
      i += 34
      els = els :+ (getTag(trace.scroll_top)+ i, 1d)
      i += 34
*/
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage + "unionLog:")
        null
    }

  }
  def getTag(y:Int):Int ={
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
    }else if (y <=7000){
      30
    }else if (y <=8000){
      31
    }else if (y <=9000){
      32
    }else if (y <=10000){
      32
    }else{
      33
    }
  }
}

