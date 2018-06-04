package com.cpc.spark.ml.userprofile

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession


/**
  * Created by roydong on 04/06/2018.
  */
object AgeModel {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("MLPC".format())
      .enableHiveSupport()
      .getOrCreate()

    // Load the data stored in LIBSVM format as a DataFrame.
    val dtrain = spark.read.format("libsvm").load("/user/cpc/qtt-list-ctr-parser2-xgboost_train_svm")
    val dtest = spark.read.format("libsvm").load("/user/cpc/qtt-list-ctr-parser2-xgboost_test_svm")

    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val layers = Array[Int](21, 10, 5, 1)


    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)

    // train the model
    val model = trainer.fit(dtrain)


    // compute accuracy on the test set
    val result = model.transform(dtest)
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
  }
}



