package com.cpc.spark.common

import com.cpc.spark.streaming.tools.Encoding

object Ui {
   def parseData(data : String) :Ui = {
     try{
     val tmps = data.split(" ")
     val d = Encoding.base64Decoder(tmps(tmps.length - 1))
     new Ui(d)
     }catch{
       case e:Exception =>
         e.printStackTrace()
         println("UI_error = " + data)
         null
     }
   }
}

class Ui(data : Seq[Byte]){
  val typed = 1
  val ui = aslog.Aslog.NoticeLogBody.parseFrom(data.toArray)
}