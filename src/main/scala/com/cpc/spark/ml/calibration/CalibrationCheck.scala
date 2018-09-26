package com.cpc.spark.ml.calibration

import java.io.FileInputStream

import com.cpc.spark.common.Utils
import com.google.protobuf.CodedInputStream
import mlmodel.mlmodel.CalibrationConfig

/**
  * author: huazhenhao
  * date: 9/19/18
  */
object CalibrationCheck {
  def main(args: Array[String]): Unit = {
    val modelPath = args(0)
    val dt = args(1)
    val hour = args(2)
    val modelName = args(3)


    println(s"modelPath=$modelPath")
    println(s"dt=$dt")
    println(s"hour=$hour")
    println(s"modelName=$modelName")

    val irModel = new CalibrationConfig().mergeFrom(CodedInputStream.newInstance(new FileInputStream(modelPath)))
    println(irModel.toString)
    val session = Utils.buildSparkSession("calibration_check")

    val timeRangeSql = Utils.getTimeRangeSql(dt, hour, dt, hour)

    // get union log
    val sql = s"""
                 | select
                 |  isclick,
                 |  ext_int['raw_ctr'] as ectr,
                 |  ext['exp_ctr'].int_value
                 | from dl_cpc.cpc_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80000001', '80000002')
                 | and isshow = 1
                 | and ext['antispam'].int_value = 0
                 | and ideaid > 0
                 | and adsrc = 1
                 | and adslot_type in (1)
                 | AND userid > 0
                 | and ext_string['ctr_model_name'] = '$modelName'
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql)
    log.limit(10).rdd.toLocalIterator.foreach( x => {
      val isClick = x.getInt(0).toDouble
      val rawCtr = x.getLong(1).toDouble / 1e6d
      val onlineCtr = x.getLong(2).toDouble / 1e6d
      val calibrated = HourlyCalibration.computeCalibration(rawCtr, irModel.ir.get)
      println(s"rawCtr: $rawCtr")
      println(s"onlineCtr: $onlineCtr")
      println(s"calibrated: $calibrated")
      println("======")
    })
    val result = log.rdd.map( x => {
      val isClick = x.getInt(0).toDouble
      val ectr = x.getLong(1).toDouble / 1e6d
      val onlineCtr = x.getLong(2).toDouble / 1e6d
      val calibrated = HourlyCalibration.computeCalibration(ectr, irModel.ir.get)
      (isClick, ectr, calibrated, 1.0, onlineCtr)
    }).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5))
    val ctr = result._1 / result._4
    val ectr = result._2 / result._4
    val calibrated_ctr = result._3 / result._4
    val onlineCtr = result._5 / result._4
    println(s"impression: ${result._4}")
    println(s"ctr: $ctr")
    println(s"ectr: $ectr")
    println(s"online ctr: $onlineCtr")
    println(s"calibrated_ctr: $calibrated_ctr")
    println(s"no calibration: ${ectr / ctr}")
    println(s"online calibration: ${onlineCtr / ctr}")
    println(s"new calibration: ${calibrated_ctr / ctr}")
  }
}
