package com.cpc.spark.ml.calibration

import java.io.FileInputStream

import com.cpc.spark.common.Utils
import com.google.protobuf.CodedInputStream
import mlmodel.mlmodel.CalibrationConfig

/**
  * author: huazhenhao
  * date: 10/09/18
  */
object CalibrationCheckCvr {
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
                 |select if(iscvr is null, 0, 1), ecvr, raw_cvr from
                 |( select searchid, ext_int['raw_cvr'] as raw_cvr, ext['exp_cvr'].int_value as ecvr
                 | from dl_cpc.cpc_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80000001', '80000002') and isclick = 1 and ext['antispam'].int_value = 0
                 | and ideaid > 0 and adsrc = 1 and adslot_type in (1, 2, 3) AND userid > 0
                 | and ext_string['cvr_model_name'] = '$modelName'
                 | and exptags like "%calibration=on%"
                 | )
                 | a left join
                 |(select searchid, label as iscvr from dl_cpc.ml_cvr_feature_v1
                 |WHERE $timeRangeSql
                 |) b on a.searchid = b.searchid
       """.stripMargin

    println(s"sql:\n$sql")
    val log = session.sql(sql)
    var ct = 0
    log.limit(1000).rdd.toLocalIterator.foreach( x => {
      val isCvr = x.getInt(0).toDouble
      val ecvr = x.getInt(1).toDouble / 1e6d
      val rawCvr = x.getLong(2).toDouble / 1e6d
      val calibrated = HourlyCalibration.computeCalibration(rawCvr, irModel.ir.get)

      if (Math.abs(ecvr - calibrated) / calibrated > 0.2) {
        println(s"isCvr: $isCvr")
        println(s"onlineCvr: $ecvr")
        println(s"calibrated: $calibrated")
        println("======")
        ct += 1
      }
    })
    println(s"error ct: $ct")

    val result = log.rdd.map( x => {
      val isCvr = x.getInt(0).toDouble
      val ecvr = x.getInt(1).toDouble / 1e6d
      val rawCvr = x.getLong(2).toDouble / 1e6d
      val calibrated = HourlyCalibration.computeCalibration(rawCvr, irModel.ir.get)
      (isCvr, ecvr, rawCvr, calibrated, 1.0)
    }).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5))
    val cvr = result._1 / result._5
    val ecvr = result._2 / result._5
    val rawCvr = result._3 / result._5
    val calibrated_cvr = result._4 / result._5
    println(s"conversion: ${result._5}")
    println(s"cvr: $cvr")
    println(s"ecvr: $ecvr")
    println(s"raw_cvr: $rawCvr")
    println(s"calibrated_cvr: $calibrated_cvr")
    println(s"no calibration: ${rawCvr / cvr}")
    println(s"online calibration: ${ecvr / cvr}")
    println(s"new calibration: ${calibrated_cvr / cvr}")
  }
}
