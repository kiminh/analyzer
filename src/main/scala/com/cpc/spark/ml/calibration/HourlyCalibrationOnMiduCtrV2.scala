package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.{CalibrationConfig, IRModel}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.IsotonicRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


object HourlyCalibrationOnMiduCtrV2 {

  val localDir = "/home/cpc/scheduled_job/hourly_calibration/"
  val destDir = "/home/work/mlcpp/calibration/"
  val newDestDir = "/home/cpc/model_server/calibration/"
  val MAX_BIN_COUNT = 20
  val MIN_BIN_SIZE = 10000

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
                 | select isclick, ext_int['raw_ctr'] as ectr, show_timestamp, ext_string['ctr_model_name'] from dl_cpc.cpc_novel_union_log
                 | where $timeRangeSql
                 | and media_appsid in ('80001098', '80001292') and isshow = 1 and ext['antispam'].int_value = 0
                 | and ideaid > 0 and adsrc = 1  AND userid > 0
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql)

    unionLogToConfig(log.rdd, session.sparkContext, softMode)
  }

  def unionLogToConfig(log: RDD[Row], sc: SparkContext, softMode: Int, saveToLocal: Boolean = true,
                       minBinSize: Int = MIN_BIN_SIZE, maxBinCount : Int = MAX_BIN_COUNT, minBinCount: Int = 5): Unit = {
    val irTrainer = new IsotonicRegression()

    val result = log.map( x => {
      var isClick = 0d
      if (x.get(0) != null) {
        isClick = x.getInt(0).toDouble
      }
      val ectr = x.getLong(1).toDouble / 1e6d
      // TODO(huazhenhao) not used right now in the first version, should be used as weights
      val model = x.getString(3)
      (model, (ectr, isClick))
    }).groupByKey()
      .mapValues(
        x =>
        (binIterable(x, minBinSize, maxBinCount), Utils.sampleFixed(x, 100000)))
      .toLocalIterator
      .map {
        x =>
          val modelName: String = x._1
          val bins = x._2._1
          val samples = x._2._2
          val size = bins._2
          val positiveSize = bins._3
          println(s"model: $modelName has data of size $size, of positive number of $positiveSize")
          println(s"bin size: ${bins._1.size}")
          if (bins._1.size <= minBinCount) {
            println("bin number too small, don't output the calibration")
            CalibrationConfig()
          } else {
            val irFullModel = irTrainer.setIsotonic(true).run(sc.parallelize(bins._1))
            val irModel = IRModel(
              boundaries = irFullModel.boundaries,
              predictions = irFullModel.predictions
            )
            println(s"bin size: ${irFullModel.boundaries.length}")
            println(s"calibration result (ectr/ctr) (before, after): ${computeCalibration(samples, irModel)}")
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
                  println(MUtils.updateMlcppModelData(localPath, newDestDir + s"calibration-$modelName.mlm", conf))
              }
            }
            config
          }
      }
  }

  // input: (<ectr, click>)
  // output: original ectr/ctr, calibrated ectr/ctr
  def computeCalibration(samples: Array[(Double, Double)], irModel: IRModel): (Double, Double) = {
    var imp = 0.0
    var click = 0.0
    var ectr = 0.0
    var calibrated = 0.0
    samples.foreach(x => {
      imp += 1
      click += x._2
      ectr += x._1
      calibrated += computeCalibration(x._1, irModel)
    })
    return (ectr / click, calibrated / click)

  }

  def binarySearch(num: Double, boundaries: Seq[Double]): Int = {
    if (num < boundaries(0)) {
      return 0
    }
    if (num >= boundaries.last) {
      return boundaries.size
    }
    val mid = boundaries.size / 2
    if (num < boundaries(mid)) {
      return binarySearch(num, boundaries.slice(0, mid))
    } else {
      return binarySearch(num, boundaries.slice(mid, boundaries.size)) + mid
    }
  }

  def computeCalibration(prob: Double, irModel: IRModel): Double = {
    if (prob <= 0) {
      return 0.0
    }
    var index = binarySearch(prob, irModel.boundaries)
    if (index == 0) {
      return Math.max(0.0, irModel.predictions(0) * (prob - irModel.boundaries(0)))
    }
    if (index == irModel.boundaries.size) {
      index = index - 1
    }
    return Math.max(0.0, Math.min(1.0, irModel.predictions(index-1) +
      (irModel.predictions(index) - irModel.predictions(index-1))
        * (prob - irModel.boundaries(index-1))
        / (irModel.boundaries(index) - irModel.boundaries(index-1))))
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
  // return: (Seq(<ctr, ectr, weight>), total count)
  def binIterable(data: Iterable[(Double, Double)], minBinSize: Int, maxBinCount: Int)
    : (Seq[(Double, Double, Double)], Double, Double) = {
    val dataList = data.toList
    val totalSize = dataList.size
    val binNumber = Math.min(Math.max(1, totalSize / minBinSize), maxBinCount)
    val binSize = totalSize / binNumber
    var bins = Seq[(Double, Double, Double)]()
    var allClickSum = 0d
    var clickSum = 0d
    var showSum = 0d
    var eCtrSum = 0d
    var n = 0
    dataList.sorted.foreach {
        x =>
          var ectr = 0.0
          if (x._1 > 0) {
            ectr = x._1
          }
          eCtrSum = eCtrSum + ectr
          if (x._2 > 1e-6) {
            clickSum = clickSum + 1
            allClickSum = allClickSum + 1
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
    return (bins, totalSize, allClickSum)
  }

  def getTimeRangeSql(startDate: String, startHour: String, endDate: String, endHour: String): String = {
    if (startDate.equals(endDate)) {
      return s"(day = '$startDate' and hour <= '$endHour' and hour >= '$startHour')"
    }
    return s"((day = '$startDate' and hour >= '$startHour') " +
      s"or (day = '$endDate' and hour <= '$endHour') " +
      s"or (day > '$startDate' and day < '$endDate'))"
  }
}