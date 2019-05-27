//package com.cpc.spark.ml.calibration
//
//import java.io.{File, FileOutputStream, PrintWriter}
//import java.time.LocalDateTime
//import java.time.format.DateTimeFormatter
//
//import com.cpc.spark.common.Utils
//import com.cpc.spark.ml.common.{Utils => MUtils}
//import com.typesafe.config.ConfigFactory
//import mlmodel.mlmodel.{CalibrationConfig, IRModel, PostCalibrations}
//import org.apache.spark.mllib.regression.IsotonicRegression
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//
//object CvrCaliTest{
//
//  val localDir = "/home/cpc/scheduled_job/hourly_calibration/"
//  val destDir = "/home/work/mlcpp/calibration/"
//  val newDestDir = "/home/cpc/model_server/calibration/"
//  val MAX_BIN_COUNT = 10
//  val MIN_BIN_SIZE = 100000
//
//  def main(args: Array[String]): Unit = {
//
//    // build spark session
//    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
//
//    // get union log
//    val sql = s"""
//                 |select * from dl_cpc.qtt_cvr_calibration_sample where dt = '2019-05-26'
//       """.stripMargin
//    println(s"sql:\n$sql")
//    val log = spark.sql(sql)
//
//    println("datasum: %d".format(log.count()))
//    println("datasum: %d".format(log.filter("iscvr = 1").count()))
//
//    val group1 = log.groupBy("ideaid","user_req_ad_num","adslot_id").count().withColumn("count1",col("count"))
//      .withColumn("group",concat_ws("_",col("ideaid"),col("user_req_ad_num"),col("adslot_id")))
//      .filter("count1>100000")
//      .select("ideaid","user_req_ad_num","adslot_id","group")
//    val group2 = log.groupBy("ideaid","user_req_ad_num").count().withColumn("count2",col("count"))
//      .withColumn("group",concat_ws("_",col("ideaid"),col("user_req_ad_num")))
//      .filter("count2>100000")
//      .select("ideaid","user_req_ad_num","group")
//    val group3 = log.groupBy("ideaid").count().withColumn("count3",col("count"))
//      .filter("count3>10000")
//      .withColumn("group",col("ideaid"))
//      .select("ideaid","group")
//
//    val data1 = log.join(group1,Seq("user_req_ad_num","adslot_id","ideaid"),"inner")
//    val data2 = log.join(group2,Seq("ideaid","user_req_ad_num"),"inner")
//    val data3 = log.join(group3,Seq("ideaid"),"inner")
//
//    //create cali pb
//    val calimap1 = GroupToConfig(data1, spark,calimodel)
//    val calimap2 = GroupToConfig(data2, spark,calimodel)
//    val calimap3 = GroupToConfig(data3, spark,calimodel)
//    val calimap = calimap1 ++ calimap2 ++ calimap3
//    val califile = PostCalibrations(calimap.toMap)
//    val localPath = saveProtoToLocal(calimodel, califile)
//    saveFlatTextFileForDebug(calimodel, califile)
//    if (softMode == 0) {
//      val conf = ConfigFactory.load()
//      println(MUtils.updateMlcppOnlineData(localPath, destDir + s"calibration-$calimodel.mlm", conf))
//      println(MUtils.updateMlcppModelData(localPath, newDestDir + s"calibration-$calimodel.mlm", conf))
//    }
//  }
//
//
//  def GroupToConfig(data:DataFrame, session: SparkSession, calimodel: String, minBinSize: Int = MIN_BIN_SIZE,
//                    maxBinCount : Int = MAX_BIN_COUNT, minBinCount: Int = 2): scala.collection.mutable.Map[String,CalibrationConfig] = {
//    val sc = session.sparkContext
//    var calimap = scala.collection.mutable.Map[String,CalibrationConfig]()
//    val result = data.select("user_req_ad_num","adslot_id","ideaid","isclick","ectr","ctr_model_name","group")
//      .rdd.map( x => {
//      var isClick = 0d
//      if (x.get(3) != null) {
//        isClick = x.getLong(3).toDouble
//      }
//      val ectr = x.getLong(4).toDouble / 1e6d
//      val model = x.getString(5)
//      val group = x.getString(6)
//      val key = calimodel + "_" + group
//      (key, (ectr, isClick))
//    }).groupByKey()
//      .mapValues(
//        x =>
//          (binIterable(x, minBinSize, maxBinCount), Utils.sampleFixed(x, 100000))
//      )
//      .toLocalIterator
//      .map {
//        x =>
//          val modelName: String = x._1
//          val bins = x._2._1
//          val samples = x._2._2
//          val size = bins._2
//          val positiveSize = bins._3
//          println(s"model: $modelName has data of size $size, of positive number of $positiveSize")
//          println(s"bin size: ${bins._1.size}")
//          if (bins._1.size < minBinCount) {
//            println("bin size too small, don't output the calibration")
//            CalibrationConfig()
//          } else {
//            val calik =
//            println(s"calibration result (ectr/ctr) (before, after): ${computeCalibration(samples, irModel)}")
//            val config = CalibrationConfig(
//              name = modelName,
//              ir = Option(irModel)
//            )
//            calimap += ((modelName,config))
//            config
//          }
//      }.toList
//    return calimap
//  }
//
//  // input: (<ectr, click>)
//  // output: original ectr/ctr, calibrated ectr/ctr
//  def computeCalibration(samples: Array[(Double, Double)], irModel: IRModel): (Double, Double) = {
//    var imp = 0.0
//    var click = 0.0
//    var ectr = 0.0
//    var calibrated = 0.0
//    samples.foreach(x => {
//      imp += 1
//      click += x._2
//      ectr += x._1
//      calibrated += computeCalibration(x._1, irModel)
//    })
//    return (ectr / click, calibrated / click)
//  }
//
//  def binarySearch(num: Double, boundaries: Seq[Double]): Int = {
//    if (num < boundaries(0)) {
//      return 0
//    }
//    if (num >= boundaries.last) {
//      return boundaries.size
//    }
//    val mid = boundaries.size / 2
//    if (num < boundaries(mid)) {
//      return binarySearch(num, boundaries.slice(0, mid))
//    } else {
//      return binarySearch(num, boundaries.slice(mid, boundaries.size)) + mid
//    }
//  }
//
//  def computeCalibration(prob: Double, irModel: IRModel): Double = {
//    if (prob <= 0) {
//      return 0.0
//    }
//    var index = binarySearch(prob, irModel.boundaries)
//    if (index == 0) {
//      return  Math.min(1.0, irModel.predictions(0) * prob/ irModel.boundaries(0))
//    }
//    if (index == irModel.boundaries.size) {
//      index = index - 1
//    }
//    return Math.max(0.0, Math.min(1.0, irModel.predictions(index-1) +
//      (irModel.predictions(index) - irModel.predictions(index-1))
//        * (prob - irModel.boundaries(index-1))
//        / (irModel.boundaries(index) - irModel.boundaries(index-1))))
//  }
//
//  def saveProtoToLocal(modelName: String, config: PostCalibrations): String = {
//    val filename = s"calibration-$modelName.mlm"
//    val localPath = localDir + filename
//    val outFile = new File(localPath)
//    outFile.getParentFile.mkdirs()
//    config.writeTo(new FileOutputStream(localPath))
//    return localPath
//  }
//
//  def saveFlatTextFileForDebug(modelName: String, config: PostCalibrations): Unit = {
//    val filename = s"calibration-flat-$modelName.txt"
//    val localPath = localDir + filename
//    val outFile = new File(localPath)
//    outFile.getParentFile.mkdirs()
//    new PrintWriter(localPath) { write(config.toString); close() }
//  }
//
//  // input: Seq<(<ectr, click>)
//  // return: (Seq(<ctr, ectr, weight>), total count)
//  def binIterable(data: Iterable[(Double, Double)], minBinSize: Int, maxBinCount: Int)
//  : (Seq[(Double, Double, Double)], Double, Double) = {
//    val dataList = data.toList
//    val totalSize = dataList.size
//    val binNumber = Math.min(Math.max(1, totalSize / minBinSize), maxBinCount)
//    val binSize = totalSize / binNumber
//    var bins = Seq[(Double, Double, Double)]()
//    var allClickSum = 0d
//    var clickSum = 0d
//    var showSum = 0d
//    var eCtrSum = 0d
//    var n = 0
//    dataList.sorted.foreach {
//      x =>
//        var ectr = 0.0
//        if (x._1 > 0) {
//          ectr = x._1
//        }
//        eCtrSum = eCtrSum + ectr
//        if (x._2 > 1e-6) {
//          clickSum = clickSum + 1
//          allClickSum = allClickSum + 1
//        }
//        showSum = showSum + 1
//        if (showSum >= binSize) {
//          val ctr = clickSum / showSum
//          bins = bins :+ (ectr, ctr/ectr, 1d)
//          n = n + 1
//          clickSum = 0d
//          showSum = 0d
//          eCtrSum = 0d
//        }
//    }
//    return (bins, totalSize, allClickSum)
//  }
//}