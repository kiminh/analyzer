package com.cpc.spark.ml.train

import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Roy on 2017/5/15.
  */
object LogisticTrain {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("logistic training"));
    sc.setLogLevel("WARN")
    val parsedData = MLUtils.loadLibSVMFile(sc, args(0))
    val splits = parsedData.randomSplit(Array(0.99,0.01), seed = 10L)

    val training = splits(0).cache()
    val test = splits(1)
    println("start training", training.count())
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)

    println("save model")
    model.save(sc, "/user/cpc/model/v1/")

    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }.cache()

    predictionAndLabels
      .toLocalIterator
      .foreach {
        x =>
          println(x._2, x._1)
      }

    val metrics = new MulticlassMetrics(predictionAndLabels)

    println(metrics.precision(1))
  }
}
