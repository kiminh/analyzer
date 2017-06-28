package com.cpc.spark.ml.train

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.{IsotonicRegression, IsotonicRegressionModel, LabeledPoint}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Created by Roy on 2017/5/15.
  */
object LRTrain {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println(
        s"""
           |Usage: Train <mode train/test> <input path> <model path> <sample rate> <p/n rate>
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger().setLevel(Level.WARN)
    val mode = args(0).trim
    val inpath = args(1).trim
    val daybefore = args(2).toInt
    val days = args(3).toInt
    val modelPath = args(4).trim
    val sampleRate = args(5).toFloat
    val pnRate = args(6).toInt
    val ctx = SparkSession.builder()
      .config("spark.driver.maxResultSize", "10G")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "com.cpc.spark.ml.train.LRRegistrator")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .config("spark.rpc.message.maxSize", "400")
      //.config("spark.speculation", "true")
      .config("spark.storage.blockManagerHeartBeatMs", "300000")
      .config("spark.scheduler.maxRegisteredResourcesWaitingTime", "100")
      .config("spark.core.connection.auth.wait.timeout", "100")

      .appName("cpc LR model %s[%s]".format(mode, modelPath))
      .getOrCreate()

    val sc = ctx.sparkContext
    val date = new SimpleDateFormat("yyyy-MM-dd").format(new Date().getTime)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)
    var pathSep = Seq[String]()
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      pathSep = pathSep :+ date
      cal.add(Calendar.DATE, 1)
    }

    val m = LogisticRegressionModel.load(sc, modelPath)

    trainCalibration(sc, m, inpath)
    println("done")
    System.exit(1)

    println("%s/{%s}".format(inpath, pathSep.mkString(",")))
    val sample = MLUtils.loadLibSVMFile(sc, "%s/{%s}".format(inpath, pathSep.mkString(",")))
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
      lbfgs.optimizer.setRegParam(0.2)
      lbfgs.optimizer.setNumCorrections(10)
      lbfgs.optimizer.setConvergenceTol(1e-4)
      */
      lbfgs.optimizer.setNumIterations(200)

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

      println("training ...", training.take(1).foreach(x => println(x.features)))
      model = lbfgs.run(training)
      model.save(sc, modelPath + "/" + date)
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

    println("predict distribution %s %d(1) %d(0)".format(testSum, test1, test0))
    predictionAndLabels
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

    // Instantiate metrics object
    val metrics = new BinaryClassificationMetrics(predictionAndLabels)

    // AUPRC
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // ROC Curve
    //val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)

    if (mode == "train") {
      val w = new PrintWriter("/home/work/ml/model/model_%s.txt".format(date))
      w.write("version 0.1\n")
      w.write("model_path %s\n".format(modelPath))
      w.write("num_features %d\n".format(model.numFeatures - 1))
      w.write("num_classes %d\n".format(model.numClasses))
      w.write("date %s\n".format(date))
      w.write("auprc %.18f\n".format(auPRC))
      w.write("aur %.18f\n".format(auROC))
      model.weights.toSparse.foreachActive {
        (i, v) =>
          w.write("%d %.18f\n".format(i, v))
      }
      w.close()
      println("IR train calibration")
      trainCalibration(sc, model, inpath)
    }

    println("all done")
    predictionAndLabels.unpersist()
    sc.stop()
  }

  def trainCalibration(sc: SparkContext, model: LogisticRegressionModel, inpath: String): Unit = {
    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    val date = fmt.format(new Date().getTime)
    val date1 = fmt.format(new Date().getTime - 3600L * 24000L)
    val sample = MLUtils.loadLibSVMFile(sc, "%s_full/%s".format(inpath, date1))
      //.randomSplit(Array(0.1, 0.9), seed = new Date().getTime)(0)
    val binNum = 1e6
    model.clearThreshold()
    println("prepare data", sample.count(), model.toString())
    val irdata = sample
      .map {
        case LabeledPoint(label, features) =>
          val prediction = model.predict(features)
          val bin = math.round(prediction * binNum)
          var click = 0
          if (label > 0.1) {
            click = 1
          }
          (bin, (click, 1))
      }
      .reduceByKey {
        (x, y) =>
          (x._1 + y._1, x._2 + y._2)
      }
      .map {
        x =>
          val v = x._2
          val ctr = v._1.toDouble / v._2.toDouble
          (ctr, x._1.toDouble / binNum.toDouble, 1.0)
      }
      .randomSplit(Array(0.9, 0.1), seed = new Date().getTime)
    println("done")

    /*
    irdata(0).sortBy(x => x._2)
      .toLocalIterator
      .foreach(println)
    */

    println("IR training...", irdata(0).count(), irdata(1).count())
    val irmodel = new IsotonicRegression()
      .setIsotonic(true)
      .run(irdata(0))
    println("done")

    val predictionAndLabel = irdata(1).map { point =>
      val predictedLabel = irmodel.predict(point._2)
      (predictedLabel, point._1)
    }

    /*
    println("results")
    predictionAndLabel
      //.randomSplit(Array(0.1, 0.9), seed = new Date().getTime)(0)
      .take(2000)
      .foreach(println)
    */
    println("predictions", irmodel.predictions.length)
    println("boundaries", irmodel.boundaries.length)

    // Calculate mean squared error between predicted and real labels.
    val meanSquaredError = predictionAndLabel.map { case (p, l) => math.pow((p - l), 2) }.mean()
    println("Mean Squared Error = " + meanSquaredError)

    // Save and load model
    val modelPath = "/user/cpc/irmodel/v1/" + date
    val w = new PrintWriter("/home/work/ml/model/irmodel_%s.txt".format(date))
    w.write("version 0.1\n")
    w.write("num_data %d\n".format(irmodel.boundaries.length))
    w.write("date %s\n".format(date))
    w.write("model_path %s\n".format(modelPath))
    w.write("mean_squared_error %.10f\n".format(meanSquaredError))
    irmodel.boundaries.indices.foreach {
      i =>
        w.write("%.6f %.6f\n".format(irmodel.boundaries(i), irmodel.predictions(i)))
    }
    w.close()
    irmodel.save(sc, modelPath)
  }
}
