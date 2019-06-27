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

    val k = 0.75
    val calimodel = "novel-cvr-dnn-rawid-v9"
    val irModel = IRModel(
      boundaries = Seq(0.0, 1.0),
      predictions = Seq(0.0, k)
    )
    println(s"k is: $k")
    val config = CalibrationConfig(
      name = calimodel,
      ir = Option(irModel)
    )
    val localPath = saveProtoToLocal(calimodel, config)
    saveFlatTextFileForDebug(calimodel, config)
  }
}