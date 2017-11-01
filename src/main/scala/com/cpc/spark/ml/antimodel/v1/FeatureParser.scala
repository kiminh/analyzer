package com.cpc.spark.ml.antimodel.v1

import java.util.Calendar

import com.cpc.spark.log.parser.{ExtValue, UnionLog}
import com.cpc.spark.ml.common.Dict
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by Roy on 2017/5/15.
  */
object FeatureParser {

  def parseUnionLog(unionLog:UnionLog ,dict: Dict): String = {
    var svm = "0"
    val vector = getVector(unionLog ,dict)
    if (vector != null) {
      var p = -1
      if(unionLog.ext.getOrElse("antispam", ExtValue()).int_value > 0){
        svm = "1"
      }
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

  def getVector(unionLog:UnionLog ,dict: Dict): Vector = {
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
      //interests
      unionLog.interests.map(dict.interest.getOrElse(_, 0))
        .filter(_ > 0)
        .sortWith(_ < _)
        .foreach {
          intr =>
            els = els :+ (intr + i - 1, 1d)
        }
      i += 200


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

      val city = dict.city.getOrElse(unionLog.city, 0)
      els = els :+ (city + i, 1d)
      i += 1000

      //ad slot id
      val slotid = dict.adslot.getOrElse(unionLog.adslotid.toInt, 0)
      els = els :+ (slotid + i, 1d)
      i += 1000

      //ad class
      var cls = 0
      if (unionLog.ext != null) {
        val v = unionLog.ext.getOrElse("adclass", null)
        if (v != null) {
          cls = v.int_value
        }
      }
     /* val adcls = dict.adclass.getOrElse(cls, 0)
      els = els :+ (adcls + i, 1d)
      i += 1000*/
      //0 to 4
      var phonelevel = unionLog.ext.getOrElse("phone_level",ExtValue()).int_value
      els = els :+ (phonelevel + i, 1d)
      i += 10

      var coin = unionLog.ext.getOrElse("share_coin",ExtValue()).int_value
      els = els :+ (getTag(coin)+ i, 1d)
      i += 47
      els = els :+ (getTag(unionLog.screen_h)+ i, 1d)
      i += 47
      els = els :+ (getTag(unionLog.screen_w)+ i, 1d)
      i += 47
      els = els :+ (getTag(unionLog.ext.getOrElse("touch_x",ExtValue()).int_value)+ i, 1d)
      i += 47
      els = els :+ (getTag(unionLog.ext.getOrElse("touch_y",ExtValue()).int_value)+ i, 1d)
      i += 47
      var betweenTime = 0
      if(unionLog.click_timestamp > 0){
        betweenTime = unionLog.click_timestamp - unionLog.timestamp
      }
      els = els :+ (getTag(betweenTime)+ i, 1d)
      i += 47
      Vectors.sparse(i, els)
    } catch {
      case e: Exception =>
        throw new Exception(els.toString + " " + i.toString + " " + e.getMessage + "unionLog:")
        null
    }

  }
  def getTag(y:Int):Int ={
    if (y<0){
      0
    }else if (y<=10){
      y
    }else if (y <= 20){
      11
    }else if (y <= 30){
      12
    }else if (y <= 40){
      13
    }else if (y <= 50){
      14
    }else if (y <= 60){
      15
    }else if (y <= 70){
      16
    }else if (y <= 80){
      17
    }else if (y <= 90){
      18
    }else if (y<= 100) {
      19
    } else if (y<= 200) {
      20
    } else if (y<= 300) {
      21
    } else if (y<= 400) {
      22
    }else if (y <= 500){
      23
    } else if (y<=600) {
      24
    } else if (y<=700) {
      25
    } else if (y<=800) {
      26
    } else if (y<=900) {
      27
    } else if (y <= 1000){
      28
    }  else if (y <= 1100){
      29
    } else if (y <= 1200){
      30
    }else if (y <= 1300){
      31
    } else if (y <= 1400){
      32
    } else if (y <= 1500){
      33
    } else if (y <= 1600){
      34
    } else if (y <= 1700){
      35
    } else if (y <= 1900){
      36
    }else if (y <= 2000){
      37
    }else if (y <= 3000){
      38
    }else if (y <= 4000){
      39
    }else if (y <= 5000){
      40
    }else if (y <=6000){
      41
    }else if (y <=7000){
      42
    }else if (y <=8000){
      43
    }else if (y <=9000){
      44
    }else if (y <=10000){
      45
    }else{
      46
    }
  }
}

