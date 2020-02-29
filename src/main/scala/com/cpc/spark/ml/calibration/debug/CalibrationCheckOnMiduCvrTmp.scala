package com.cpc.spark.ml.calibration.debug

import java.io.{File, FileInputStream, PrintWriter}
import scala.sys.process._
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
    val modelPath = "hdfs://emr-cluster/warehouse/dl_cpc.db/cpc_algo_models/"+ args(0)

    println(s"modelPath=$modelPath")
    val file = "/home/cpc/last_calbration.mlm"
    s"hdfs dfs -get $modelPath $file" !


    val calimap = new PostCalibrations().mergeFrom(CodedInputStream.newInstance(new FileInputStream(file))).caliMap


    println(calimap.toString())


  }
}
