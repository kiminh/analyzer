package com.cpc.spark.ml.server

import com.cpc.spark.ml.common.Utils
import com.typesafe.config.ConfigFactory

/**
  * Created by roydong on 07/08/2017.
  */
object UpdateModelFile {

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load()
    Utils.updateOnlineData("/data/cpc/anal/model/" + args(0), args(1), conf)
    Utils.updateOnlineData("/data/cpc/anal/model/" + args(2), args(3), conf)

  }

}
