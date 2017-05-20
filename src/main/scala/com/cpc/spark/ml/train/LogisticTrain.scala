package com.cpc.spark.ml.train

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
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
      //random pick 1/20 negative sample
      .filter(x => x.label > 0.01 || Random.nextInt(20) == 1)
      .map {
        x =>
          new LabeledPoint(label = x.label, features = nor.transform(x.features))
      }
      .randomSplit(Array(0.99,0.01), seed = 1314159L)

    val training = splits(0).cache()
    val test = splits(1)

    println("sample ratio", training.count())
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

    print("training ...")
    new LogisticRegressionWithLBFGS
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(training)
    training.unpersist()
    println("done")

    print("testing...")
    model.clearThreshold()
    val predictionAndLabels = test.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }.cache()
    println("done")

    println("1000 results and predict avg")
    predictionAndLabels.take(1000).foreach(println)
    predictionAndLabels
      .map {
        x =>
          var label = 0
          if (x._2 > 0.01) {
            label = 1
          }
          (label, (x._1, 1))
      }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .toLocalIterator
      .foreach {
        x =>
          val sum = x._2
          println(x._1, sum._2, sum._1 / sum._2.toDouble)
      }

    predictionAndLabels.unpersist()
    println("save model")
    model.save(sc, "/user/cpc/model/v1/")
    sc.stop()
  }
}
