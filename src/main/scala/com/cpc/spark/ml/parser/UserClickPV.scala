package com.cpc.spark.ml.parser

import scala.collection.mutable
import scala.io.Source

/**
  * Created by roydong on 2017/6/10.
  */
trait UserClickPV {

  val userClk = mutable.Map[String, Int]()

  val userPV = mutable.Map[String, Int]()

  def loadUserInfo(path: String): Unit = {
    for (line <- Source.fromFile(path, "UTF8").getLines()) {
      val row = line.split("\t")
      if (row.length == 3) {
        userClk.update(row(0), row(1).toInt)
        userPV.update(row(0), row(2).toInt)
      }
    }
  }
}
