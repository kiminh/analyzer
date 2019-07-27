package com.cpc.spark.ml.calibration

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.calibration.HourlyCalibration._
import mlmodel.mlmodel.{CalibrationConfig, IRModel}
import org.apache.log4j.{Level, Logger}


object KFactorCaliv2 {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    // parse and process input

    val k = args(0).toDouble
    val calimodel = args(1)
    val irModel = IRModel(
      boundaries = Seq(1.0),
      predictions = Seq(k)
    )
    println(s"calimodel is: $calimodel")
    println(s"k is: $k")
    val config = CalibrationConfig(
      name = calimodel,
      ir = Option(irModel)
    )
    val localPath = saveProtoToLocal(calimodel, config)
    saveFlatTextFileForDebug(calimodel, config)
  }
}