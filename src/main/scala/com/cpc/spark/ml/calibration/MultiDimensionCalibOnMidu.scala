package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.calibration.HourlyCalibration._
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.{CalibrationConfig, IRModel}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.IsotonicRegression
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.cpc.spark.ml.common.{Utils => MUtils}
import org.apache.spark.sql.functions._


object MultiDimensionCalibOnMidu {

  val localDir = "/home/cpc/scheduled_job/hourly_calibration/"
  val destDir = "/home/work/mlcpp/calibration/"
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

    val timeRangeSql = Utils.getTimeRangeSql_2(startDate, startHour, endDate, endHour)

    // get union log
    val sql = s"""
                 |select isclick, cast(raw_ctr as bigint) as ectr, show_timestamp, ctr_model_name, adslot_id, ideaid,
                 |case when user_req_ad_num = 1 then '1'
                 |  when user_req_ad_num = 2 then '2'
                 |  when user_req_ad_num in (3,4) then '4'
                 |  when user_req_ad_num in (5,6,7) then '7'
                 |  else '8' end as user_req_ad_num
                 | from dl_cpc.cpc_novel_union_events
                 | where $timeRangeSql
                 | and media_appsid in ('80001098', '80001292') and isshow = 1
                 | and ctr_model_name = 'novel-ctr-dnn-rawid-v7'
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql)
    log.persist()

    val group1 = log.groupBy("user_req_ad_num","adslot_id","ideaid").count()
      .withColumn("count1",col("count"))

    group1.printSchema()

    group1.show(5)

    val group2 = log.groupBy("user_req_ad_num","adslot_id").count().withColumn("count2",col("count"))

    val group3 = log.groupBy("user_req_ad_num").count().withColumn("count3",col("count"))

    val keygroup = group1.join(group2,Seq("user_req_ad_num","adslot_id"),"left").join(group3,Seq("user_req_ad_num"),"left")
        .withColumn("group",concat_ws("_",col("user_req_ad_num"),col("adslot_id"),col("ideaid")))
        .withColumn("group",when(col("count1") < 10000,concat_ws("_",col("user_req_ad_num"),col("adslot_id")))
          .otherwise("group"))
        .withColumn("group",when(col("count2") < 10000,col("user_req_ad_num"))
         .otherwise("group"))

    keygroup.printSchema()
    keygroup.select("user_req_ad_num","adslot_id","ideaid","group").distinct().show(100)

    val data = log.

//    unionLogToConfig2(log.rdd, session.sparkContext, softMode)
  }


  def unionLogToConfig2(log: RDD[Row], sc: SparkContext, softMode: Int, saveToLocal: Boolean = true,
                       minBinSize: Int = MIN_BIN_SIZE, maxBinCount : Int = MAX_BIN_COUNT, minBinCount: Int = 5): List[CalibrationConfig] = {
    val irTrainer = new IsotonicRegression()

    val result = log.map( x => {
      var isClick = 0d
      if (x.get(0) != null) {
        isClick = x.getInt(0).toDouble
      }
      val ectr = x.getLong(1).toDouble / 1e6d
      val model = x.getString(3)
      val adslot_id = x.getString(4)
      val ideaid = x.getInt(5).toString
      val key = model+'_'+adslot_id+'_'+ideaid
      (key, (ectr, isClick))
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
//          println(s"model: $modelName has data of size $size, of positive number of $positiveSize")
//          println(s"bin size: ${bins._1.size}")
          if (bins._1.size <= minBinCount) {
//            println("bin number too small, don't output the calibration")
            CalibrationConfig()
          } else {
            println(s"model: $modelName has data of size $size, of positive number of $positiveSize")
            println(s"bin size: ${bins._1.size}")
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
      }.toList
    return result
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
}