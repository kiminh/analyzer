package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import org.apache.spark.mllib.regression.IsotonicRegression
import com.cpc.spark.common.Utils
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.{CalibrationConfig, IRModel}
import com.cpc.spark.ml.common.{Utils => MUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object HourlyCalibration {

  val localDir = "/home/cpc/scheduled_job/hourly_calibration/"
  val destDir = "/home/work/mlcpp/calibration/"
  val MAX_BIN_COUNT = 500
  val MIN_BIN_SIZE = 100

  def main(args: Array[String]): Unit = {

    // parse and process input
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val softMode = args(3).toInt


    val endTime = LocalDateTime.parse(s"$endDate-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))

    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    println(s"endDate=$endDate")
    println(s"endHour=$endHour")
    println(s"hourRange=$hourRange")
    println(s"startDate=$startDate")
    println(s"startHour=$startHour")
    println(s"softMode=$softMode")

    // build spark session
    val session = Utils.buildSparkSession("hourlyCalibration")

    val timeRangeSql = Utils.getTimeRangeSql(startDate, startHour, endDate, endHour)

    // get union log
    val sql = s"""
                 | select isclick, ext_int['raw_ctr'] as ectr, show_timestamp, ext_string['ctr_model_name'] from dl_cpc.cpc_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80000001', '80000002') and isshow = 1 and ext['antispam'].int_value = 0
                 | and ideaid > 0 and adsrc = 1 and adslot_type in (1) AND userid > 0
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql)


    unionLogToConfig(log.rdd, session.sparkContext, softMode)
  }

  def unionLogToConfig(log: RDD[Row], sc: SparkContext, softMode: Int, saveToLocal: Boolean = true,
                       minBinSize: Int = MIN_BIN_SIZE, maxBinCount : Int = MAX_BIN_COUNT): List[CalibrationConfig] = {
    val irTrainer = new IsotonicRegression()

    val result = log.map( x => {
      val isClick = x.getInt(0).toDouble
      val ectr = x.getLong(1).toDouble / 1e6d
      // TODO(huazhenhao) not used right now in the first version, should be used as weights
      // val showTimeStamp = x.getAs[Int]("show_timestamp")
      val model = x.getString(3)
      (model, (ectr, isClick))
    }).groupByKey()
      .mapValues(binIterable(_, minBinSize, maxBinCount))
      .toLocalIterator
      .map {
        x =>
          val modelName = x._1
          val irFullModel = irTrainer.setIsotonic(true).run(sc.parallelize(x._2))
          val irModel = IRModel(
            boundaries = irFullModel.boundaries,
            predictions = irFullModel.predictions
          )
          val config = CalibrationConfig(
            name = modelName,
            ir = Option(irModel)
          )
          if (saveToLocal) {
            val localPath = saveProtoToLocal(modelName, config)
            saveFlatTextFileForDebug(modelName, config)
            if (softMode == 0) {
              val conf = ConfigFactory.load()
              println(MUtils.updateMlcppOnlineData(localPath, destDir + s"calibration-$modelName.mlm", conf))
            }
          }
          config
      }.toList
    return result
  }

  def saveProtoToLocal(modelName: String, config: CalibrationConfig): String = {
    val filename = s"calibration-$modelName.mlm"
    val localPath = localDir + filename
    val outFile = new File(localPath)
    outFile.getParentFile.mkdirs()
    config.writeTo(new FileOutputStream(localPath))
    return localPath
  }

  def saveFlatTextFileForDebug(modelName: String, config: CalibrationConfig): Unit = {
    val filename = s"calibration-flat-$modelName.txt"
    val localPath = localDir + filename
    val outFile = new File(localPath)
    outFile.getParentFile.mkdirs()
    new PrintWriter(localPath) { write(config.toString); close() }
  }

  // input: Seq<(<ectr, click>)
  // return: Seq(<ctr, ectr, weight>)
  def binIterable(data: Iterable[(Double, Double)], minBinSize: Int, maxBinCount: Int)
    : Seq[(Double, Double, Double)] = {
    val dataList = data.toList
    val totalSize = dataList.size
    val binNumber = Math.min(Math.max(1, totalSize / minBinSize), maxBinCount)
    val binSize = dataList.size / binNumber
    var bins = Seq[(Double, Double, Double)]()
    var clickSum = 0d
    var showSum = 0d
    var eCtrSum = 0d
    var n = 0
    dataList.sorted.foreach {
        x =>
          eCtrSum = eCtrSum + x._1
          if (x._2 > 1e-6) {
            clickSum = clickSum + 1
          }
          showSum = showSum + 1
          if (showSum >= binSize) {
            val ctr = clickSum / showSum
            bins = bins :+ (ctr, eCtrSum / showSum, 1d)
            n = n + 1
            clickSum = 0d
            showSum = 0d
            eCtrSum = 0d
          }
      }
    return bins
  }
}