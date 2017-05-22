package com.cpc.spark.ml.train

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.optimization.{LogisticGradient, SquaredL2Updater}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Random

/**
  * Created by Roy on 2017/5/15.
  */
object LogisticTrain {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println(s"""
        |Usage: Train <mode train/test> <input path> <model path> <sample rate> <p/n rate>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger().setLevel(Level.WARN)
    val ctx = SparkSession.builder()
      .appName("cpc training logistic model")
      .getOrCreate()
    val sc = ctx.sparkContext
    val mode = args(0).trim
    val inpath = args(1).trim
    val modelPath = args(2).trim
    val sampleRate = args(3).toFloat
    val pnRate = args(4).toInt
    val parsedData = MLUtils.loadLibSVMFile(sc, inpath)
    val nor = new Normalizer()
    val splits = parsedData
      //random pick 1/pnRate negative sample
      .filter(x => x.label > 0.01 || Random.nextInt(pnRate) == 0)
      .map(x => new LabeledPoint(x.label, x.features))
      .randomSplit(Array(sampleRate, 1 - sampleRate), seed = 1314159L)

    val test = splits(1)
    var model: LogisticRegressionModel = null
    if (mode == "test") {
      model = LogisticRegressionModel.load(sc, modelPath)
    } else {
      val training = splits(0).cache()
      val lbfgs = new LogisticRegressionWithLBFGS()
        .setNumClasses(2)
        .setIntercept(true)
      /*
      lbfgs.optimizer.setGradient(new LogisticGradient())
      lbfgs.optimizer.setUpdater(new SquaredL2Updater())
      lbfgs.optimizer.setNumCorrections(10)
      lbfgs.optimizer.setNumIterations(100)
      */
      lbfgs.optimizer.setConvergenceTol(0.001)
      lbfgs.optimizer.setRegParam(0.1)

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

      println("training ...")
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

    println("predict distribution total %s".format(predictionAndLabels.count()))
    predictionAndLabels
      .map {
        x =>
          (("%.1f".format(x._1), x._2.toInt), 1)
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
          println("%s %d %d %.4f".format(x._1, sum._2, sum._1, sum._2.toDouble / (sum._1 + sum._2).toDouble))
      }

    if (modelPath.length == 0) {
      ctx.createDataFrame(predictionAndLabels)
        .write
        .mode(SaveMode.Append)
        .text("/user/cpc/test_result")

      predictionAndLabels.unpersist()
      println("save model")
      model.save(sc, modelPath)
      sc.stop()
    }
  }
}
