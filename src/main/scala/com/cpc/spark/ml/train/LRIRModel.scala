package com.cpc.spark.ml.train

import java.io.PrintWriter
import java.util.{Calendar, Date}

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.optimization.{L1Updater, LBFGS, LogisticGradient, SquaredL2Updater}
import org.apache.spark.mllib.regression.{IsotonicRegression, IsotonicRegressionModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.util.Arrays.binarySearch

/**
  * Created by Roy on 2017/5/15.
  * LogisticRegression + IsotonicRegression
  */
class LRIRModel {

  private var ctx: SparkSession = null

  def initSpark(appName: String): SparkSession = {
    ctx = SparkSession.builder()
      .config("spark.driver.maxResultSize", "20G")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "com.cpc.spark.ml.train.LRRegistrator")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .config("spark.rpc.message.maxSize", "400")
      .config("spark.network.timeout", "240s")
      //.config("spark.speculation", "true")
      .config("spark.storage.blockManagerHeartBeatMs", "300000")
      .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "100")
      .config("spark.core.connection.auth.wait.timeout", "100")
      .appName(appName)
      .getOrCreate()
    ctx
  }

  def stopSpark(): Unit = {
    ctx.stop()
  }

  private var lrmodel: LogisticRegressionModel = null

  def loadLRmodel(hdfsPath: String): Unit = {
    lrmodel = LogisticRegressionModel.load(ctx.sparkContext, hdfsPath)
  }

  def getLRmodel(): LogisticRegressionModel = {
    lrmodel
  }

  def run(sample: RDD[LabeledPoint], iterNum: Int, tol: Double): Unit = {
    val lbfgs = new LogisticRegressionWithLBFGS().setNumClasses(2)
    /*
    lbfgs.optimizer.setGradient(new LogisticGradient())
    lbfgs.optimizer.setRegParam(0.2)
    lbfgs.optimizer.setNumCorrections(10)
    */
    lbfgs.optimizer.setUpdater(new L1Updater())
    if (iterNum > 0) {
      lbfgs.optimizer.setNumIterations(iterNum)
    }
    if (tol > 0) {
      lbfgs.optimizer.setConvergenceTol(tol)
    }

    lrmodel = lbfgs.run(sample)
    lrmodel.clearThreshold()
  }

  def saveHdfs(path: String): Unit = {
    lrmodel.save(ctx.sparkContext, path)
  }

  def saveText(path: String): Unit = {
    if (lrmodel == null) {
      throw new Exception("must train lr first")
    }
    val w = new PrintWriter(path)
    w.write("version 0.1\n")
    w.write("num_features %d\n".format(lrmodel.numFeatures - 1))
    w.write("num_classes %d\n".format(lrmodel.numClasses))
    w.write("auprc %.18f\n".format(auPRC))
    w.write("aur %.18f\n".format(auROC))
    w.write("\r\n")
    lrmodel.weights.toSparse.foreachActive {
      (i, v) =>
        w.write("%d %.18f\n".format(i, v))
    }
    w.close()
  }

  /*
  (prediction, label)
   */
  private var lrTestResults: RDD[(Double, Double)] = null

  def getLrTestResults(): RDD[(Double, Double)] = {
    lrTestResults
  }

  private var auPRC = 0d

  def getAuPRC(): Double = {
    auPRC
  }

  private var auROC = 0d

  def getAuROC(): Double = {
    auROC
  }

  def test(sample: RDD[LabeledPoint]): Unit = {
    if (lrmodel == null) {
      throw new Exception("must train lr first")
    }
    val lr = lrmodel
    lr.clearThreshold()
    lrTestResults = sample.map{case LabeledPoint(label, features) => (lr.predict(features), label)}

    // Instantiate metrics object
    val metrics = new BinaryClassificationMetrics(lrTestResults)

    // AUPRC
    auPRC = metrics.areaUnderPR

    // ROC Curve
    //val roc = metrics.roc

    // AUROC
    auROC = metrics.areaUnderROC
  }

  def printLrTestLog(): Unit = {
    val testSum = lrTestResults.count()
    if (testSum < 0) {
      throw new Exception("must run lr test first or test results is empty")
    }
    var test0 = 0
    var test1 = 0
    lrTestResults
      .map {
        x =>
          var label = 0
          if (x._2 > 0.01) {
            label = 1
          }
          (label, 1)
      }
      .reduceByKey((x, y) => x + y)
      .toLocalIterator
      .foreach {
        x =>
          if (x._1 == 1) {
            test1 = x._2
          } else {
            test0 = x._2
          }
      }

    println("predict distribution %s %d(1) %d(0)".format(testSum, test1, test0))
    lrTestResults
      .map {
        x =>
          val v = (x._1 * 100).toInt / 5
          ((v, x._2.toInt), 1)
      }
      .reduceByKey((x, y) => x + y)
      .map {
        x =>
          val key = x._1._1
          val label = x._1._2
          if (label == 0) {
            (key, (x._2, 0))
          } else {
            (key, (0, x._2))
          }
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          val sum = x._2
          println("%.2f %d %.4f %.4f %d %.4f %.4f %.4f".format(x._1.toDouble * 0.05,
            sum._2, sum._2.toDouble / test1.toDouble, sum._2.toDouble / testSum.toDouble,
            sum._1, sum._1.toDouble / test0.toDouble, sum._1.toDouble / testSum.toDouble,
            sum._2.toDouble / (sum._1 + sum._2).toDouble))
      }

    println("auPRC: %.10f, auROC: %.10f".format(auPRC, auROC))
  }

  private var irmodel: IsotonicRegressionModel = null

  def getIRmodel(): IsotonicRegressionModel = {
    irmodel
  }

  private var irBinNum = 0
  private var irError = 0d

  def runIr(binNum: Int, rate: Double): Double = {
    if (lrTestResults == null) {
      throw new Exception("must run lr and test first")
    }
    irBinNum = binNum
    val sample = lrTestResults.randomSplit(Array(rate, 1 - rate), seed = new Date().getTime)
    val bins = binData(sample(0), irBinNum)
    val sc = ctx.sparkContext
    val ir = new IsotonicRegression().setIsotonic(true).run(sc.parallelize(bins.map(x => (x._1, x._3, 1d))))
    val sum = sample(1)
      .map(x => (x._2, ir.predict(x._1)))   //(ctr, calibrate ctr)
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    irError = (sum._2 - sum._1) / sum._1  //误差比
    irmodel = ir
    irError
  }

  def saveIrText(path: String): Unit = {
    if (irmodel == null) {
      throw new Exception("must run ir first")
    }
    val w = new PrintWriter(path)
    w.write("version 0.1\n")
    w.write("num_data %d\n".format(irmodel.boundaries.length))
    w.write("bin_num %d\n".format(irBinNum))
    w.write("mean_error %.10f\n".format(irError))
    w.write("\r\n")
    irmodel.boundaries.indices.foreach {
      i =>
        w.write("%.10f %.10f\n".format(irmodel.boundaries(i), irmodel.predictions(i)))
    }
    w.close()
  }

  /*
  @return ctr minPrediction avgPrediction maxPrediction
   */
  private def binData(sample: RDD[(Double, Double)], binNum: Int): Seq[(Double, Double, Double, Double)] = {
    val binSize = sample.count().toInt / binNum
    var bins = Seq[(Double, Double, Double, Double)]()
    var click = 0d
    var pv = 0d
    var pSum = 0d
    var pMin = 1d
    var pMax = 0d
    var n = 0
    sample.sortByKey()
      .toLocalIterator
      .foreach {
        x =>
          pSum = pSum + x._1
          if (x._1 < pMin) {
            pMin = x._1
          }
          if (x._1 > pMax) {
            pMax = x._1
          }
          if (x._2 > 0.01) {
            click = click + 1
          }
          pv = pv + 1
          if (pv >= binSize) {
            val ctr = click / pv
            bins = bins :+ (ctr, pMin, pSum / pv, pMax)
            n = n + 1
            if (n >= binNum - 100) {
              println("  bin %d: %.6f(%d/%d) %.6f %.6f %.6f".format(
                n, ctr, click.toInt, pv.toInt, pMin, pSum / pv, pMax))
            }

            click = 0d
            pv = 0d
            pSum = 0d
            pMin = 1d
            pMax = 0d
          }
      }
    bins
  }

  var boundaries = Array[Double]()
  var predictions = Array[Double]()
  var caliBinNum = 0
  var caliError = 0d

  def runCalibrateData(binNum: Int, rate: Double, rateTest: Double): Double = {
    if (lrTestResults == null) {
      throw new Exception("must run lr and test first")
    }
    caliBinNum = binNum
    val sample = lrTestResults.randomSplit(
      Array(rate, rateTest, 1 - rate - rateTest), seed = new Date().getTime)
    val bins = binData(sample(0), caliBinNum)
    var n = 0
    boundaries = new Array[Double](bins.length)
    predictions = new Array[Double](bins.length)
    bins.foreach {
      x =>
        boundaries(n) = x._4  //prediction max
        predictions(n) = x._1   //ctr
        n = n + 1
    }
    val sum = sample(1).toLocalIterator
      .map {
        x =>
          val cali = calibrate(x._1)
          (x._2, cali, 1)
      }
      .reduce {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2, x._3 + y._3)
      }

    val ctr = sum._1 / sum._3
    val caliCtr = sum._2 / sum._3
    caliError = caliCtr / ctr
    caliError
  }

  def saveCaliText(path: String): Unit = {
    val w = new PrintWriter(path)
    w.write("version 0.1\n")
    w.write("num_data %d\n".format(boundaries.length))
    w.write("bin_num %d\n".format(caliBinNum))
    w.write("mean_squared_error %.10f\n".format(irError))
    w.write("\r\n")
    boundaries.indices.foreach {
      i =>
        w.write("%.10f %.10f\n".format(boundaries(i), predictions(i)))
    }
    w.close()
  }

  def calibrate(testData: Double): Double = {

    def linearInterpolation(x1: Double, y1: Double, x2: Double, y2: Double, x: Double): Double = {
      y1 + (y2 - y1) * (x - x1) / (x2 - x1)
    }

    val foundIndex = binarySearch(boundaries, testData)
    val insertIndex = -foundIndex - 1

    // Find if the index was lower than all values,
    // higher than all values, in between two values or exact match.
    if (insertIndex == 0) {
      predictions.head
    } else if (insertIndex == boundaries.length) {
      predictions.last
    } else if (foundIndex < 0) {
      linearInterpolation(
        boundaries(insertIndex - 1),
        predictions(insertIndex - 1),
        boundaries(insertIndex),
        predictions(insertIndex),
        testData)
    } else {
      predictions(foundIndex)
    }
  }
}
