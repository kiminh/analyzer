package com.cpc.spark.ml.train

import java.util.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Created by Roy on 2017/5/15.
  */
object LRTrain {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println(s"""
        |Usage: Train <mode train/test> <input path> <model path> <sample rate> <p/n rate>
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger().setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .config("spark.driver.maxResultSize", "2G")
      .appName("cpc training LR model v5")
      .getOrCreate()
    val sc = ctx.sparkContext
    val mode = args(0).trim
    val inpath = args(1).trim
    val modelPath = args(2).trim
    val sampleRate = args(3).toFloat
    val pnRate = args(4).toInt

    val sample = MLUtils.loadLibSVMFile(sc, inpath)
      //random pick 1/pnRate negative sample
      .filter(x => x.label > 0.01 || Random.nextInt(pnRate) == 0)
      .randomSplit(Array(sampleRate, 1 - sampleRate), seed = new Date().getTime)

    val test = sample(1)
    var model: LogisticRegressionModel = null
    if (mode == "test") {
      model = LogisticRegressionModel.load(sc, modelPath)
    } else {
      val lbfgs = new LogisticRegressionWithLBFGS().setNumClasses(2)
      /*
      lbfgs.optimizer.setGradient(new LogisticGradient())
      lbfgs.optimizer.setUpdater(new SquaredL2Updater())
      lbfgs.optimizer.setNumIterations(100)
      lbfgs.optimizer.setRegParam(0.2)
      lbfgs.optimizer.setNumCorrections(10)
      */
      lbfgs.optimizer.setConvergenceTol(1e-4)

      val training = sample(0).cache()
      println("sample count", training.count())
      training
        .map {
          x =>
            var label = 0
            if (x.label > 0.01) {
              label = 1
            }
            (label, 1)
        }
        .reduceByKey((x, y) => x + y)
        .toLocalIterator
        .foreach(println)

      println("training ...", training.take(1).foreach(x=>println(x.features)))
      model = lbfgs.run(training)
      println("done")
      training.unpersist()
    }

    println("testing...")
    model.clearThreshold()
    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }.cache()
    println("done")


    val testSum = predictionAndLabels.count()
    var test0 = 0
    var test1 = 0
    predictionAndLabels
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

    println("predict distribution total %s %d(1) %d(0)".format(testSum, test1, test0))
    predictionAndLabels
      .map {
        x =>
          (("%.1f".format(Math.abs(x._1 - 0.05)), x._2.toInt), 1)
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
          println("%s %d %.4f %.4f %d %.4f %.4f %.4f".format( x._1,
            sum._2, sum._2.toDouble / test1.toDouble, sum._2.toDouble / testSum.toDouble,
            sum._1, sum._1.toDouble / test0.toDouble, sum._1.toDouble / testSum.toDouble,
            sum._2.toDouble / (sum._1 + sum._2).toDouble))
      }

    if (mode == "train") {
      println("save model")
      model.save(sc, modelPath)
      predictionAndLabels.unpersist()
    }

    sc.stop()
  }
}


