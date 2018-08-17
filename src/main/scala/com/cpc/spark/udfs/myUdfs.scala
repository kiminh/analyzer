package com.cpc.spark.udfs

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, WrappedArray}

object myUdfs {

  def udfArrayToString: UserDefinedFunction = {
    udf[String, Seq[String]]((value: Seq[String]) => value.mkString(""))
  }

  def udfArrayToJSONString: UserDefinedFunction = {
    udf[String, Seq[String]]((value: Seq[String]) => value.mkString("[", ",", "]"))
  }

  def nameValue(name: String) = udf((value: String) => {
    if (value == null) {
      null
    } else {
      s"$name:$value"
    }
  })

  def nameValueToIndexValue(map: mutable.LinkedHashMap[String, String]) = udf((value: String) => {
    if (value == null) {
      null
    } else {
      val array = value.split(" ")
      var list = new ListBuffer[String]
      for (a <- array) {
        val items = a.split(":")
        val feaname = items(0).trim
        if (map.contains(feaname)) {
          val newitem = map(items(0).trim) + ":" + items(1)
          list += newitem
        }
      }
      list.mkString(" ")
    }
  })

  /*def jsonStrToProtobuf(nameIndexMap: Map[String, Long]): UserDefinedFunction = udf((str: String) => {
    val proto = strToProto(str, nameIndexMap)
    proto.toByteArray
  })

  def strToProto(str: String, nameIndexMap: Map[String, Long]): ProtoPortrait = {
    val json = JSON.parseObject(str)
    var sMap = mutable.LinkedHashMap[String, ProtoPortrait]()
    var vMap = mutable.LinkedHashMap[Long, Float]()
    for (k <- json.keySet()) {
      if (k.contains("#")) {
        sMap += (k -> jsonArrayToProtoPortrait(k, json.getJSONArray(k), nameIndexMap))
      }
      else {
        if (nameIndexMap.contains(k)) {
          vMap += (nameIndexMap(k) -> json.getDouble(k).floatValue())
        }
      }
    }
    var proto = ProtoPortrait(
      subMap = sMap.toMap,
      valueMap = vMap.toMap
    )
    proto
  }

  def jsonArrayToProtoPortrait(key: String, jSONArray: JSONArray, nameIndexMap: Map[String, Long]): ProtoPortrait = {
    val dims = key.split("#")
    val second = dims(1)
    var map = mutable.LinkedHashMap[String, ProtoPortrait]()
    for (i <- 0 until jSONArray.size()) {
      val obj = jSONArray.getJSONObject(i)
      if (obj.containsKey(second) && obj.get(second) != null) {
        val id = obj.get(second).toString
        map += (id -> jsonObjectToProto(id, obj, nameIndexMap))
      }
    }
    var proto = ProtoPortrait(
      subMap = map.toMap
    )
    proto
  }

  def jsonObjectToProto(key: String, jsonObject: JSONObject, nameIndexMap: Map[String, Long]): ProtoPortrait = {

    var map = mutable.LinkedHashMap[Long, Float]()
    for (k <- jsonObject.keySet()) {
      if (!k.equals(key)) {
        if (nameIndexMap.contains(k)) {
          map += (nameIndexMap(k) -> jsonObject.getDoubleValue(k).floatValue())
        }
      }
    }
    var proto = ProtoPortrait(
      valueMap = map.toMap
    )
    proto
  }*/

  def udfPkgArrayToLibsvm(nameIndexMap: Map[String, String]) = udf((value: WrappedArray[String]) => {

    val array = value.map(x => {
      if (nameIndexMap.contains(x)) {
        nameIndexMap(x) + ":1.0"
      } else {
        null
      }
    })
    var buf = new StringBuilder
    for (x <- array) {
      if (x != null) {
        buf ++= " " + x
      }
    }
    buf.toString().trim
  })

  def libsvmToVec(size: Int): UserDefinedFunction = udf((libsvm: String) => {

    var els = Seq[(Int, Double)]()
    if (libsvm.trim != "") {
      libsvm.split("\\s+").map(x => {
        val v = x.split(":")
        els = els :+ (v(0).toInt, v(1).toDouble)
      })
      val vec = Vectors.sparse(size, els)
      vec
    } else {
      val vec = Vectors.sparse(size, els)
      vec
    }


  })

  def predictToLabel(thresh: Double): UserDefinedFunction = udf((predict: Double) => {

    if (predict >= thresh) {
      1.0
    } else {
      0.0
    }

  })


  def udfParseDayOfWeek = udf((logtime: String) => {
    val calendar = Calendar.getInstance
    //    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    calendar.setTime(dateFormat.parse(logtime))
    calendar.get(Calendar.DAY_OF_WEEK)
  })


  def mapToString() = udf((value: Map[Int, Float]) => {
    if (value.isEmpty() == true) {
      null
    } else {
      var list = new ListBuffer[String]
      for (k <- value.keySet) {
        val newitem = k.toString.trim + ":" + value(k).toString.trim
        list += newitem
      }
      list.mkString(" ")
    }
  })

  def intToString() = udf((value: Int) => {
    value.toString
  })

  def checkStudent() = udf((value: String) => {
    if(value.contains("224=100")){
      "student"
    }
    else if (value.contains("225=100")){
      "not_student"
    }
    else{
      "notag"
    }

  })


  def checkZfb() = udf((valueMain: String) => {
    if(valueMain==null){
      "0"
    }
    else{
      "1"
    }

  })

  def zfbsextostring() = udf((zfbsex: BigInt) => {
    zfbsex.toString()
  })
  def unisextostring() = udf((unisex: Int) => {
    unisex.toString()
  })

  def setsex() = udf((iszfb: String,zfbsex: String,unisex :String) => {
    if(iszfb ==null){
      null
    }
    else {
      if (iszfb.contains("1")) {
          zfbsex
      }
      else {
          unisex
      }
    }
  })
  def operationTag() = udf(() => {
      true
  })

  def ChangeCvrStringToInt() = udf((clickNum: String) => {
    1

  })
  def downloadTag(ctrThres :Double,cvrThres :Double) = udf((clickNum: Int,iscvrNum: Int,showNum :Int) => {

    var clickNum1=0.0
    var iscvrNum1=0.0
    var showNum=0.0

    var uidctr=0.0
    var uidcvr=0.0

    if(clickNum==null)
      clickNum1=0
    else
      clickNum1=clickNum

    if(iscvrNum==null)
      iscvrNum1=0
    else
      iscvrNum1=iscvrNum

    if(showNum==0){
      uidctr=0.0
    }
    else{
      uidctr = clickNum1/(showNum+0.0)
    }
    if(clickNum1==0){
      uidcvr=0.0
    }
    else{
      uidcvr = iscvrNum1/(clickNum1+0.0)
    }


    if(uidctr>=ctrThres && uidcvr>=cvrThres )
      "11".toInt
    else if(uidctr>=ctrThres && uidcvr<cvrThres )
      "10".toInt
    else if(uidctr<ctrThres && uidcvr>=cvrThres )
      "01".toInt
    else
      "00".toInt
  })
  def birthdayZfb() = udf((valueInf: String) => {
    if (valueInf == null) {
      null
    }
    else {
      if(valueInf.contains("person_birthday")){
        val index = valueInf.indexOf("person_birthday")
        valueInf(index+18).toString+valueInf(index+19).toString+valueInf(index+20).toString+valueInf(index+21).toString+valueInf(index+22).toString+valueInf(index+23).toString+valueInf(index+24).toString+valueInf(index+25).toString
        }
        else {
        null
        }
      }

  })


  def clickNum(uidTimeMap: mutable.LinkedHashMap[String, String]) = udf((valueClickString: String) => {
    val myArray = valueClickString.split(" ")
    println(myArray.size)
    val valueClick = myArray(0)
    val valueUid = myArray(1)
    val valueTime = myArray(2)
    //println(valueTime)
    if (uidTimeMap.contains(valueUid)) {
      if (uidTimeMap(valueUid) > valueTime) {
        if (valueClick == "1") {
          11.toString
        }
        else {
          10.toString
        }
      }
      else if (uidTimeMap(valueUid) < valueTime) {
        if (valueClick == "1") {
          21.toString
        }
        else {
          20.toString
        }
      }
      else {
        null
      }
    }
    else {
      null
    }
  })

  def iscvrNum(uidTimeMap: mutable.LinkedHashMap[String, String]) = udf((valueClickString: String) => {
      val myArray = valueClickString.split(" ")
      println(myArray.size)
      val valueClick = myArray(0)
      val valueUid = myArray(1)
      val valueTime = myArray(2)
      //println(valueTime)
      if (uidTimeMap.contains(valueUid)) {
        if (uidTimeMap(valueUid) > valueTime) {
          if (valueClick == "1") {
            11.toString
          }
          else {
            10.toString
          }
        }
        else if (uidTimeMap(valueUid) < valueTime) {
          if (valueClick == "1") {
            21.toString
          }
          else {
            20.toString
          }
        }
        else
        {
          null
        }
      }
      else {
        null
      }
  })



  def clickNum1(uidTimeMap: mutable.LinkedHashMap[String, String]) = udf((valueClick: Int, valueUid: String,valueTime:Int) => {
    val valueTime1= valueTime.toString
    if (uidTimeMap.contains(valueUid)) {
        if (uidTimeMap(valueUid) >= valueTime1) {
          if (valueClick == 1) {
            11.toString
          }
          else {
            10.toString
          }
        }
        else if (uidTimeMap(valueUid) < valueTime1) {
          if (valueClick == 1) {
            21.toString
          }
          else {
            20.toString
          }
        }
        else
        {
          null
        }
      }
      else {
        null
      }

  })

  def iscvrNum1(uidTimeMap: mutable.LinkedHashMap[String, String]) = udf((valueClick: String, valueUid: String,valueTime:Int) => {


      //println(valueTime)
    val valueTime1= valueTime.toString
      if (uidTimeMap.contains(valueUid)) {
        if (uidTimeMap(valueUid) >= valueTime1) {
          if (valueClick == "1") {
            11.toString
          }
          else {
            10.toString
          }
        }
        else if (uidTimeMap(valueUid) < valueTime1) {
          if (valueClick == "1") {
            21.toString
          }
          else {
            20.toString
          }
        }
        else
        {
          null
        }
      }
      else {
        null
      }

  })

}

