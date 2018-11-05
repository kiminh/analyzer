package com.cpc.spark.common




/**
  * Created on ${Date} ${Time}
  */
object FmClickData {
  def parseData(line: Array[Byte]): FmClickData = {
    new FmClickData(line)
  }
}

class FmClickData(data: Array[Byte]) {
//  val log = Mlevent.AdActionEvent.parseFrom(data)
}
