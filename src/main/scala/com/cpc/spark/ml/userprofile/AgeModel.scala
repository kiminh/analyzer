package com.cpc.spark.ml.userprofile

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
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

    import spark.implicits._

    // Load the data stored in LIBSVM format as a DataFrame.
    //val dtrain = spark.read.format("libsvm").load("/user/cpc/qtt-list-ctr-parser2-xgboost_train_svm")
    //val dtest = spark.read.format("libsvm").load("/user/cpc/qtt-list-ctr-parser2-xgboost_test_svm")

    val dtrain = MLUtils.loadLibSVMFile(spark.sparkContext, "/user/cpc/qtt-list-ctr-parser2-xgboost_train_svm")
      .map {
        x =>
          val vec = Vectors.dense(x.features.toArray)
          (x.label, vec)
      }
      .toDF("label", "features")
    val dtest = MLUtils.loadLibSVMFile(spark.sparkContext, "/user/cpc/qtt-list-ctr-parser2-xgboost_test_svm")
      .map {
        x =>
          val vec = Vectors.dense(x.features.toArray)
          (x.label, vec)
      }
      .toDF("label", "features")

    println(dtrain.first())

    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    val layers = Array[Int](22, 10, 5, 2)
    val thresholds = Array[Double](0, 0, 0, 0)

    println(10 * 22 + 5 * 10 + 2 * 5 + 2)


    // create the trainer and set its parameters
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)


    // train the model
    val model = trainer.fit(dtrain)

    println("weights: ", model.weights.size)
    model.weights
      .foreachActive {
        (i, v) =>
          println(i, v)
      }


    // compute accuracy on the test set
    val result = model.transform(dtest)
    val predictionAndLabels = result.select("prediction", "label")
    //val predictionAndLabels = result.select("probability", "label")
    val testResults = predictionAndLabels.rdd.map(x => (x.getDouble(0), x.getDouble(1)))


    printXGBTestLog(testResults)
    val metrics = new BinaryClassificationMetrics(testResults)
    val auPRC = metrics.areaUnderPR
    val auROC = metrics.areaUnderROC
    println("auPRC=%.6f auROC=%.6f".format(auPRC, auROC))


    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    println(s"Test set accuracy = ${evaluator.evaluate(predictionAndLabels)}")
  }

  def printXGBTestLog(lrTestResults: RDD[(Double, Double)]): Unit = {
    val testSum = lrTestResults.count()
    if (testSum < 0) {
      throw new Exception("must run lr test first or test results is empty")
    }
    var test0 = 0 //反例数
    var test1 = 0 //正例数
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

    var log = "predict distribution %s %d(1) %d(0)\n".format(testSum, test1, test0)
    lrTestResults  //(p, label)
      .map {
      x =>
        val v = (x._1 * 100).toInt / 5
        ((v, x._2.toInt), 1)
    }  //  ((预测值,lable),1)
      .reduceByKey((x, y) => x + y)  //  ((预测值,lable),num)
      .map {
      x =>
        val key = x._1._1
        val label = x._1._2
        if (label == 0) {
          (key, (x._2, 0))  //  (预测值,(num1,0))
        } else {
          (key, (0, x._2))  //  (预测值,(0,num2))
        }
    }
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))  //  (预测值,(num1反,num2正))
      .sortByKey(false)
      .toLocalIterator
      .foreach {
        x =>
          val sum = x._2
          val pre = x._1.toDouble * 0.05
          if (pre>0.2) {
            //isUpdateModel = true
          }
          log = log + "%.2f %d %.4f %.4f %d %.4f %.4f %.4f\n".format(
            pre, //预测值
            sum._2, //正例数
            sum._2.toDouble / test1.toDouble, //该准确率下的正例数/总正例数
            sum._2.toDouble / testSum.toDouble, //该准确率下的正例数/总数
            sum._1,  //反例数
            sum._1.toDouble / test0.toDouble, //该准确率下的反例数/总反例数
            sum._1.toDouble / testSum.toDouble, //该准确率下的反例数/总数
            sum._2.toDouble / (sum._1 + sum._2).toDouble)  // 真实值
      }

    println(log)
  }
}



