package com.cpc.spark.qukan.utils

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vectors

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import scala.collection.mutable.WrappedArray

object Udfs {

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

  def udfSnapshotToLeafFeatures: UserDefinedFunction = udf((feature: Map[Int, Float]) => {
    var list = new ListBuffer[String]()
    for ((k, v) <- feature) {
      val leafId = k * Math.pow(2, 11) + v
      list.append(s"${leafId.toInt}")
    }
    list.mkString(" ")
  })

  def udfIntToIndex(map: Map[Int, Int]): UserDefinedFunction = udf((value: Int) => {
   if(map.contains(value)) {
     map(value)
   } else {
     0
   }
  })

  def udfStrToIndex(map: Map[String, Int]): UserDefinedFunction = udf((value: String) => {
    if(map.contains(value)) {
      map(value)
    } else {
      0
    }
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
  def checkStudent2() = udf((value: String) => {
    if(value.contains("239")){
      "student"
    }
    else if(value.contains("240")){
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
}

