package com.cpc.spark.ml.calibration

import java.io.FileInputStream

import com.cpc.spark.common.Utils
import com.google.protobuf.CodedInputStream
import mlmodel.mlmodel.CalibrationConfig

/**
  * author: wangyao
  * date: 9/19/18
  */
object CalibrationCheckOnMidu {
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
                 |  ext['exp_ctr'].int_value,
                 |  searchid
                 | from dl_cpc.cpc_novel_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80001098', '80001292')
                 | and isshow = 1
                 | and ext['antispam'].int_value = 0
                 | and ideaid > 0
                 | and adsrc = 1
                 | and userid > 0
                 | and ext_string['ctr_model_name'] = '$modelName'
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql)
    log.limit(1000).rdd.toLocalIterator.foreach( x => {
      val isClick = x.getInt(0).toDouble
      val rawCtr = x.getLong(1).toDouble / 1e6d
      val onlineCtr = x.getInt(2).toDouble / 1e6d
      val searchid = x.getString(3)
      val calibrated = HourlyCalibration.computeCalibration(rawCtr, irModel.ir.get)

      if (Math.abs(onlineCtr - calibrated) / calibrated > 0.2) {
        println(s"rawCtr: $rawCtr")
        println(s"onlineCtr: $onlineCtr")
        println(s"calibrated: $calibrated")
        println(s"searchid: $searchid")
        println("======")
      }

    })
    val result = log.rdd.map( x => {
      val isClick = x.getInt(0).toDouble
      val ectr = x.getLong(1).toDouble / 1e6d
      val onlineCtr = x.getInt(2).toDouble / 1e6d
      val calibrated = HourlyCalibration.computeCalibration(ectr, irModel.ir.get)
      var mistake = 0
      if (Math.abs(onlineCtr - calibrated) / calibrated > 0.2) {
        mistake = 1
      }
      (isClick, ectr, calibrated, 1.0, onlineCtr, mistake)
    }).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5, x._6 + y._6))
    val ctr = result._1 / result._4
    val ectr = result._2 / result._4
    val calibrated_ctr = result._3 / result._4
    val onlineCtr = result._5 / result._4
    println(s"impression: ${result._4}")
    println(s"mistake: ${result._6}")
    println(s"ctr: $ctr")
    println(s"ectr: $ectr")
    println(s"online ctr: $onlineCtr")
    println(s"calibrated_ctr: $calibrated_ctr")
    println(s"no calibration: ${ectr / ctr}")
    println(s"online calibration: ${onlineCtr / ctr}")
    println(s"new calibration: ${calibrated_ctr / ctr}")
  }
}
