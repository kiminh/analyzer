package com.cpc.spark.ml.ftrl

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.{IsotonicRegression, IsotonicRegressionModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}


/**
  * Created by zhaolei on 22/12/2017.
  */
object XgboostLrTest {

  //  private val minBinSize = 10000d
  private var binNum = 1000d
  private var binsLog = Seq[String]()
  private var irBinNum = 0
  private var irError = 0d
  private var isUpdateModel = false
  private var xgbTestResults: RDD[(Double, Double)] = null
  private var trainLog = Seq[String]()
  private var irmodel: IsotonicRegressionModel = _
  private var lrmodel: LogisticRegressionModel = _
  private var auPRC = 0d
  private var auROC = 0d

  def modelClear(): Unit = {
    binsLog = Seq[String]()
    irBinNum = 0
    irError = 0
    xgbTestResults = null
    irmodel = null
    isUpdateModel = false
  }

  def main(args: Array[String]): Unit = {

    val size = 1000000

    var inputname = s"/user/cpc/qtt-portrait-ftrl/sample_for_ftrl/ftrl-2018-09-15-12-list-ctr-9.svm"
    println(s"inputname = $inputname")

    Logger.getRootLogger.setLevel(Level.WARN)
    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("xgboost ->lr train test")
      .enableHiveSupport()
      .getOrCreate()

    val qttListTestLeaf: RDD[LabeledPoint] = spark.sparkContext
      .textFile(inputname, 50)
      .map { x => {
        val array = x.split("\t")
        val label = array(0).toDouble
        val vector1 = array(1).split("\\s+").map(x => {
          val array = x.split(":")
          (array(0).toInt, array(1).toDouble)
        })
        val vec = Vectors.sparse(size, vector1)
        LabeledPoint(label, vec)
      }
      }



    //val Array(train, test) = qttListLeaf.randomSplit(Array(0.8, 0.2))
    val train = qttListTestLeaf
    val test = qttListTestLeaf

    println(s"train size = ${train.count()}")
    println(s"test size = ${test.count()}")

    val lbfgs = new LogisticRegressionWithLBFGS().setNumClasses(2)
    lbfgs.optimizer.setNumIterations(50)
    lbfgs.optimizer.setConvergenceTol(1e-30)
    val sampleTrain = train
    val sampleTest = test


//    val pSize = 20
    val lr = lbfgs.run(sampleTrain.union(sampleTrain))
    val filetime = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)
    lr.clearThreshold()
    xgbTestResults = sampleTest.map { r => (lr.predict(r.features), r.label) }
    // printXGBTestLog(xgbTestResults)
    val metrics = new BinaryClassificationMetrics(xgbTestResults)
    auPRC = metrics.areaUnderPR
    auROC = metrics.areaUnderROC
    trainLog :+= "auPRC=%.6f auROC=%.6f".format(auPRC, auROC)
    println(s"test: auPrc=$auPRC, auc=$auROC")
    lrmodel = lr

    // train metrics
    val xgbTrainResults = sampleTrain.map { r => (lr.predict(r.features), r.label) }
    val metricsTrain = new BinaryClassificationMetrics(xgbTrainResults)
    println(s"train: auPrc=${metricsTrain.areaUnderPR()}, auc=${metricsTrain.areaUnderROC()}")



  }



}

