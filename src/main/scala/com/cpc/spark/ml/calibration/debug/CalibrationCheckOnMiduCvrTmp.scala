package com.cpc.spark.ml.calibration.debug

import java.io.{File, FileInputStream, PrintWriter}

import com.cpc.spark.common.Utils
import com.google.protobuf.CodedInputStream
import mlmodel.mlmodel.{IRModel, PostCalibrations}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat_ws, udf, _}
//import com.cpc.spark.ml.calibration.MultiDimensionCalibOnQtt.computeCalibration

/**
  * author: wangyao
  * date: 5/14/19
  */
object CalibrationCheckOnMiduCvrTmp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val modelPath = args(0)
    val dt = args(1)
    val hour = args(2)
    val modelName = args(3)


    println(s"modelPath=$modelPath")


    val calimap = new PostCalibrations().mergeFrom(CodedInputStream.newInstance(new FileInputStream(modelPath))).caliMap

    val localPath = "/home/cpc/wy/novel_v8_postcali.txt"
    val outFile = new File(localPath)
    outFile.getParentFile.mkdirs()
    new PrintWriter(localPath) {
      write(calimap.toString); close()
    }

    val modelset = calimap.keySet
    val session = Utils.buildSparkSession("calibration_check")

  }
}
