package com.cpc.spark.ml.calibration

import java.io.FileInputStream
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils
import com.google.protobuf.CodedInputStream
import mlmodel.mlmodel.CalibrationConfig

/**
  * author: huazhenhao
  * date: 10/09/18
  */
object CalibrationCheckCvrFull {
  def main(args: Array[String]): Unit = {
    val modelPath = args(0)
    val dt = args(1)
    val hour = args(2)
    val hourRange = args(3).toInt
    val modelName = args(4)


    println(s"modelPath=$modelPath")
    println(s"dt=$dt")
    println(s"hour=$hour")
    println(s"range=$hourRange")
    println(s"modelName=$modelName")

    val endTime = LocalDateTime.parse(s"$dt-$hour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))

    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    val irModel = new CalibrationConfig().mergeFrom(CodedInputStream.newInstance(new FileInputStream(modelPath)))
    println(irModel.toString)
    val session = Utils.buildSparkSession("calibration_check")

    val timeRangeSql = Utils.getTimeRangeSql(startDate, startHour, dt, hour)

    // get union log
   val sql = s"""
                 |select
                 |  if(iscvr is null, 0, iscvr),
                 |  ecvr,
                 |  raw_cvr from
                 |( select searchid, ext_int['raw_cvr'] as raw_cvr, ext['exp_cvr'].int_value as ecvr
                 | from dl_cpc.cpc_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80000001', '80000002')
                 | and isclick = 1
                 | and ext['antispam'].int_value = 0
                 | and ideaid > 0
                 | and adsrc = 1
                 | and adslot_type in (1, 2, 3)
                 | and ext_string['cvr_model_name'] = '$modelName'
                 | and userid > 0
                 | and (ext["charge_type"] IS NULL
                 |       OR ext["charge_type"].int_value = 1)
                 | and round(ext["adclass"].int_value/1000) != 132101
                 | )
                 | a left join
                 |(select searchid, label as iscvr from dl_cpc.ml_cvr_feature_v1
                 |WHERE $timeRangeSql
                 |) b on a.searchid = b.searchid
       """.stripMargin

    println(s"sql:\n$sql")
    val log = session.sql(sql)
    val result = log.rdd.map( x => {
      val isCvr = x.getInt(0).toDouble
      var ecvr = x.getInt(1).toDouble / 1e6d
      if (ecvr < 0.0) {
        ecvr = 0.0
      }
      var rawCvr = x.getLong(2).toDouble / 1e6d
      if (rawCvr < 0.0) {
        rawCvr = 0.0
      }
      val calibrated = HourlyCalibration.computeCalibration(rawCvr, irModel.ir.get)
      (isCvr, ecvr, rawCvr, calibrated, 1.0)
    }).reduce((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3, x._4 + y._4, x._5 + y._5))
    val cvr = result._1 / result._5
    val ecvr = result._2 / result._5
    val rawCvr = result._3 / result._5
    val calibrated_cvr = result._4 / result._5
    println(s"conversion: ${result._1}")
    println(s"cvr: $cvr")
    println(s"online cvr: $ecvr")
    println(s"raw_cvr: $rawCvr")
    println(s"calibrated_cvr: $calibrated_cvr")
    println(s"no calibration: ${rawCvr / cvr}")
    println(s"online calibration: ${ecvr / cvr}")
    println(s"new calibration: ${calibrated_cvr / cvr}")
  }
}
