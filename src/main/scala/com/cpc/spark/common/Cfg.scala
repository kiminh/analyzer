package com.cpc.spark.common

import com.cpc.spark.streaming.tools.Encoding

/**
  * Created by zhaogang on 2017/12/15.
  */
object Cfg {
  def parseData(data : String) :Cfg = {
    try{
      val tmps = data.split(" ")
      val d = Encoding.base64Decoder(tmps(tmps.length - 1))
      new Cfg(d)
    }catch{
      case e:Exception =>
        e.printStackTrace()
        println("cfg_error = " + data)
        null
    }
  }
}
class Cfg(data : Seq[Byte]){
  val typed = 1
  val cfg = cfglog.Cfglog.NoticeLogBody.parseFrom(data.toArray)
}