package com.cpc.spark.ml.calibration

import java.io.{File, FileOutputStream, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import com.cpc.spark.common.Utils
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.{CalibrationConfig, IRModel, PostCalibrations}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.IsotonicRegression
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object MultiDimensionCalibrationOnQttCtr {

  val localDir = "/home/cpc/scheduled_job/hourly_calibration/"
  val destDir = "/home/work/mlcpp/calibration/"
  val MAX_BIN_COUNT = 10
  val MIN_BIN_SIZE = 100000

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    // parse and process input
    val endDate = args(0)
    val endHour = args(1)
    val hourRange = args(2).toInt
    val media = args(3)
    val calimodel = args(4)
    val model = args(5)
    val adslot_type = args(6)
    val conf = ConfigFactory.load("ocpc")
    val conf_key = "medias." + media + ".media_selection"
    val mediaSelection = conf.getString(conf_key)


    val endTime = LocalDateTime.parse(s"$endDate-$endHour", DateTimeFormatter.ofPattern("yyyy-MM-dd-HH"))
    val startTime = endTime.minusHours(Math.max(hourRange - 1, 0))

    val startDate = startTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
    val startHour = startTime.format(DateTimeFormatter.ofPattern("HH"))

    println(s"endDate=$endDate")
    println(s"endHour=$endHour")
    println(s"hourRange=$hourRange")
    println(s"startDate=$startDate")
    println(s"startHour=$startHour")

    // build spark session
    val session = Utils.buildSparkSession("hourlyCalibration")
    val timeRangeSql = Utils.getTimeRangeSql_3(startDate, startHour, endDate, endHour)

    // get union log
    val sql = s"""
                 |select cast(isclick as int) isclick, cast(raw_ctr as bigint) as ectr, substring(adclass,1,6) as adclass,
                 |ctr_model_name as model, adslotid, cast(ideaid as string) ideaid,
                 |case
                 |  when user_req_ad_num = 0 then '0'
                 |  when user_req_ad_num = 1 then '1'
                 |  when user_req_ad_num = 2 then '2'
                 |  when user_req_ad_num in (3,4) then '4'
                 |  when user_req_ad_num in (5,6,7) then '7'
                 |  else '8' end as user_req_ad_num
                 | from dl_cpc.slim_union_log
                 | where $timeRangeSql
                 | and $mediaSelection and isshow = 1 and adslot_type = $adslot_type
                 | and ctr_model_name in ('$model','$calimodel')
                 | and ideaid > 0 and adsrc = 1 AND userid > 0
                 | AND (charge_type IS NULL OR charge_type = 1)
       """.stripMargin
    println(s"sql:\n$sql")
    val log = session.sql(sql)
    log.show(10)
    LogToPb(log, session, calimodel)
  }

  def LogToPb(log:DataFrame, session: SparkSession, model: String)={
    val group1 = log.groupBy("adclass","ideaid","user_req_ad_num","adslotid").count().withColumn("count1",col("count"))
      .withColumn("group",concat_ws("_",col("adclass"),col("ideaid"),col("user_req_ad_num"),col("adslotid")))
      .filter("count1>100000")
      .select("adclass","ideaid","user_req_ad_num","adslotid","group")
    val group2 = log.groupBy("adclass","ideaid","user_req_ad_num").count().withColumn("count2",col("count"))
      .withColumn("group",concat_ws("_",col("adclass"),col("ideaid"),col("user_req_ad_num")))
      .filter("count2>100000")
      .select("adclass","ideaid","user_req_ad_num","group")
    val group3 = log.groupBy("adclass","ideaid").count().withColumn("count3",col("count"))
      .filter("count3>100000")
      .withColumn("group",concat_ws("_",col("adclass"),col("ideaid")))
      .select("adclass","ideaid","group")
    val group4 = log.groupBy("adclass").count().withColumn("count4",col("count"))
      .filter("count4>10000")
      .withColumn("group",col("adclass"))
      .select("adclass","group")

    val data1 = log.join(group1,Seq("adclass","ideaid","user_req_ad_num","adslotid"),"inner")
    val calimap1 = GroupToConfig(data1, session,model)

    val data2 = log.join(group2,Seq("adclass","ideaid","user_req_ad_num"),"inner")
    val calimap2 = GroupToConfig(data2, session,model)

    val data3 = log.join(group3,Seq("adclass","ideaid"),"inner")
    val calimap3 = GroupToConfig(data3, session,model)

    val data4 = log.join(group4,Seq("adclass"),"inner")
    val calimap4 = GroupToConfig(data4, session,model)

    val calimap5 = GroupToConfig(log.withColumn("group",lit("0")), session,model)
    val calimap = calimap1 ++ calimap2 ++ calimap3 ++ calimap4 ++ calimap5
    val califile = PostCalibrations(calimap.toMap)
    val localPath = saveProtoToLocal2(model, califile)
    saveFlatTextFileForDebug2(model, califile)
  }

  def GroupToConfig(data:DataFrame, session: SparkSession, model: String, minBinSize: Int = MIN_BIN_SIZE,
                    maxBinCount : Int = MAX_BIN_COUNT, minBinCount: Int = 2): scala.collection.mutable.Map[String,CalibrationConfig] = {
    val irTrainer = new IsotonicRegression()
    val sc = session.sparkContext
    var calimap = scala.collection.mutable.Map[String,CalibrationConfig]()
    val result = data.select("isclick","ectr","model","group")
      .rdd.map( x => {
      var isClick = 0d
      if (x.get(0) != null) {
        isClick = x.getInt(0).toDouble
      }
      val ectr = x.getLong(1).toDouble / 1e6d
      val model = x.getString(2)
      val group = x.getString(3)
      val key = group
      (key, (ectr, isClick))
    }).groupByKey()
      .mapValues(
        x =>
          (binIterable(x, minBinSize, maxBinCount), Utils.sampleFixed(x, 100000))
      )
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
          if (bins._1.size < minBinCount) {
            println("bin size too small, don't output the calibration")
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
            calimap += ((modelName,config))
            config
          }
      }.toList
    return calimap
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
      return  Math.min(1.0, irModel.predictions(0) * prob/ irModel.boundaries(0))
    }
    if (index == irModel.boundaries.size) {
      index = index - 1
    }
    return Math.max(0.0, Math.min(1.0, irModel.predictions(index-1) +
      (irModel.predictions(index) - irModel.predictions(index-1))
        * (prob - irModel.boundaries(index-1))
        / (irModel.boundaries(index) - irModel.boundaries(index-1))))
  }

  def saveProtoToLocal2(modelName: String, config: PostCalibrations): String = {
    val filename = s"post-calibration-$modelName.mlm"
    val localPath = localDir + filename
    val outFile = new File(localPath)
    outFile.getParentFile.mkdirs()
    config.writeTo(new FileOutputStream(localPath))
    return localPath
  }

  def saveFlatTextFileForDebug2(modelName: String, config: PostCalibrations): Unit = {
    val filename = s"post-calibration-flat-$modelName.txt"
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
    val binNumber = Math.min(Math.max(2, totalSize / minBinSize), maxBinCount)
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