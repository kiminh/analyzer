package com.cpc.spark.ml.train

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

/**
  * Created by Roy on 2017/5/15.
  */
object LogisticTrain {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger().setLevel(Level.WARN)
    val sc = new SparkContext(new SparkConf().setAppName("logistic training"));
    val parsedData = MLUtils.loadLibSVMFile(sc, args(0))
    val nor = new Normalizer()
    val splits = parsedData
      .filter(x => x.label > 0.01 || Random.nextInt(1000) > 990)
      .map{
        x =>
          new LabeledPoint(label = x.label, features = nor.transform(x.features))
      }
      .randomSplit(Array(0.9,0.1), seed = 10L)

    val training = splits(0)
    val test = splits(1)

    println("start training", training.count())
    new LogisticRegressionWithLBFGS
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)


    model.clearThreshold()
    println("save model")
    model.save(sc, "/user/cpc/model/v1/")

    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }.cache()


    var psum = 0D
    var pcnt = 0

    var nsum = 0D
    var ncnt = 0
    println("print 1000")
    predictionAndLabels.take(1000).foreach(println)
    predictionAndLabels.toLocalIterator
      .foreach {
        x =>
          if (x._2 > 0.01) {
            psum += x._1
            pcnt += 1
          } else {
            nsum += x._1
            ncnt += 1
          }
      }

    println("positive", psum, pcnt, psum / pcnt.toDouble)
    println("negative", nsum, ncnt, nsum / pcnt.toDouble)

    val metrics = new MulticlassMetrics(predictionAndLabels)
    println(metrics.precision(1))
  }
}
