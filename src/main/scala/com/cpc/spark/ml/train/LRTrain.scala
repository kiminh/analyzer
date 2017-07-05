package com.cpc.spark.ml.train

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.{IsotonicRegression, LabeledPoint}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import java.util.Arrays.binarySearch
import sys.process._

import scala.util.Random

/**
  * Created by Roy on 2017/5/15.
  */
object LRTrain {

  def main(args: Array[String]): Unit = {
    if (args.length < 5) {
      System.err.println(
        s"""
           |Usage: Train <mode:train/test[+ir]>
           |  <svmPath:string> <dayBefore:int> <day:int>
           |  <modelPath:string> <sampleRate:float> <PNRate:int>
           |  <IRBinNum:int>
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val mode = args(0).trim
    val inpath = args(1).trim
    val daybefore = args(2).toInt
    val days = args(3).toInt
    val modelPath = args(4).trim
    val sampleRate = args(5).toFloat
    val pnRate = args(6).toInt
    val binNum = args(7).toInt
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
    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    val date = fmt.format(new Date().getTime)
    val yesterday = fmt.format(new Date().getTime - 3600L * 24000L)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)

    var model: LogisticRegressionModel = null
    if (mode.startsWith("test")) {
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

      var pathSep = Seq[String]()
      for (n <- 1 to days) {
        val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
        pathSep = pathSep :+ date
        cal.add(Calendar.DATE, 1)
      }
      println("%s/{%s}".format(inpath, pathSep.mkString(",")))
      val sample = MLUtils.loadLibSVMFile(sc, "%s/{%s}".format(inpath, pathSep.mkString(",")))
        //random pick 1/pnRate negative sample
        .filter(x => x.label > 0.01 || Random.nextInt(pnRate) == 0)
        .randomSplit(Array(sampleRate, 1 - sampleRate), seed = new Date().getTime)(0)
        .cache()

      println("sample count", sample.count())
      sample
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

      println("training ...", sample.take(1).foreach(x => println(x.features)))
      model = lbfgs.run(sample)
      model.save(sc, modelPath + "/" + date)
      sample.unpersist()
      println("done")
    }

    println("testing...")
    model.clearThreshold()
    val predictionAndLabels = MLUtils.loadLibSVMFile(sc, "%s_full/%s".format(inpath, yesterday))
      .randomSplit(Array(sampleRate, 1 - sampleRate), seed = new Date().getTime)(0)
      .map {
        case LabeledPoint(label, features) =>
          (model.predict(features), label)
      }
      .cache()

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

    if (mode.startsWith("train")) {
      val filepath = "/home/cpc/anal/ctrmodel/logistic_%s.txt".format(date)
      val w = new PrintWriter(filepath)
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

      //满足条件的模型直接替换线上数据
      if (auPRC > 0.05 && auROC > 0.8) {
        val ret  = s"cp $filepath /home/work/ml/model/logistic.txt" !
        val ret1 = s"scp $filepath work@cpc-bj01:/home/work/ml/model/logistic.txt" !
        val ret2 = s"scp $filepath work@cpc-bj05:/home/work/ml/model/logistic.txt" !
      }
    }

    if (mode.endsWith("+ir")) {
      println("IR train calibration")
      trainCalibration(sc, predictionAndLabels, binNum, date)
    }

    println("all done")
    predictionAndLabels.unpersist()
    sc.stop()
  }

  def trainCalibration(sc: SparkContext, predictions: RDD[(Double, Double)], binNum: Int, date: String): Unit = {
    println("prepare ir model data...")
    val rate = 0.9
    val sample = predictions.randomSplit(Array(rate, 1 - rate), seed = new Date().getTime)

    println("binning data")
    val bins = binData(sample(0), binNum)
    println("done")

    println("IR training...", bins.length)
    val ir = new IsotonicRegression().setIsotonic(true).run(sc.parallelize(bins))
    println("done")

    //test
    println("testing...")
    val sum = sample(1).map { x =>
      val cali = ir.predict(x._1)
      (x._2, cali, 1)
    }
    .reduce { (x, y) =>
      (x._1 + y._1, x._2 + y._2, x._3 + y._3)
    }
    val ctr = sum._1 / sum._3
    val caliCtr = sum._2 / sum._3
    val errorPercent = (sum._2 - sum._1) / sum._1
    val error = caliCtr - ctr
    println("done", sum, errorPercent, ctr, caliCtr)

    val filepath = "/home/cpc/anal/ctrmodel/isotonic_%s.txt".format(date)
    val w = new PrintWriter(filepath)
    w.write("version 0.1\n")
    w.write("num_data %d\n".format(ir.boundaries.length))
    w.write("date %s\n".format(date))
    w.write("bin_num %d\n".format(binNum))
    w.write("mean_squared_error %.10f\n".format(error))
    ir.boundaries.indices.foreach {
      i =>
        w.write("%.10f %.10f\n".format(ir.boundaries(i), ir.predictions(i)))
    }
    w.close()

    if (error < 0.0001) {
      val ret = s"cp $filepath /home/work/ml/model/isotonic.txt" !
      val ret1 = s"scp $filepath work@cpc-bj01:/home/work/ml/model/isotonic.txt" !
      val ret2 = s"scp $filepath work@cpc-bj05:/home/work/ml/model/isotonic.txt" !
    }
  }

  def binData(sample: RDD[(Double, Double)], binNum: Int): Seq[(Double, Double, Double)] = {
    val binSize = sample.count().toInt / binNum
    var bins = Seq[(Double, Double, Double)]()
    var click = 0d
    var pv = 0d
    var pSum = 0d
    var pMin = 1d
    var n = 0
    sample.sortByKey()
      .toLocalIterator
      .foreach {
        x =>
          pSum = pSum + x._1
          if (x._1 < pMin) {
            pMin = x._1
          }
          if (x._2 > 0.01) {
            click = click + 1
          }
          pv = pv + 1
          if (pv >= binSize) {
            val ctr = click / pv
            bins = bins :+ (ctr, pSum / pv, 1.0)
            n = n + 1
            if (n >= binNum - 150) {
              println("  bin %d: %.6f(%d/%d) %.6f %.6f".format(n, ctr, click.toInt, pv.toInt, pMin, pSum / pv))
            }

            click = 0d
            pv = 0d
            pSum = 0d
            pMin = 1d
          }
      }
    bins
  }

  def getCalibrate(boundaries: Array[Double], predictions: Array[Double], v: Double): Double = {
    def linearInterpolation(x1: Double, y1: Double, x2: Double, y2: Double, x: Double): Double = {
      y1 + (y2 - y1) * (x - x1) / (x2 - x1)
    }

    val foundIndex = binarySearch(boundaries, v)
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
        v)
    } else {
      predictions(foundIndex)
    }
  }
}
