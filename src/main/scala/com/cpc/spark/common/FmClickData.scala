package com.cpc.spark.common

import data.Mlevent


/**
  * Created on ${Date} ${Time}
  */
object FmClickData {
  def parseData(line: Array[Byte]): FmClickData = {
    new FmClickData(line)
  }
}

class FmClickData(data: Array[Byte]) {
  val log = Mlevent.AdActionEvent.parseFrom(data)
}
