package com.cpc.spark.common

import data.Data

object LogData {
  def parseData(line:Array[Byte]):LogData = {
    new LogData(line)
  } 
}

class LogData(data :Array[Byte] ){
  val log = Data.Log.parseFrom(data)
}