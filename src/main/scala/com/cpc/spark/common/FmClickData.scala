package com.cpc.spark.common


/**
  * Created on ${Date} ${Time}
  */
object FmClickData {
  def parseData(line:Array[Byte]):LogData = {
    new LogData(line)
  }
}

class FmClickData(data :Array[Byte] ){

}
