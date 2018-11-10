package com.cpc.spark.common

import data.mlevent

/**
  * Created on 2018-11-05
  */
object FmClickData {
  def parseData(line: Array[Byte]): FmClickData = {
    new FmClickData(line)
  }
}

class FmClickData(data: Array[Byte]) {
  val log = mlevent.AdActionEvent.parseFrom(data)
}
