package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.intel.imllib.fm.regression.{FMModel, FMWithSGD}
import org.apache.spark.sql.Dataset

//import com.intel.imllib.fm.regression.{FMModel, FMWithSGD}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{DenseMatrix, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier

import scala.util.Random

/**
  * Created by roydong on 19/07/2017.
  */
object CvrFM {

  def main(args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)

    val inpath = args(0).trim
    val daybefore = args(1).toInt
    val days = args(2).toInt

    val ctx = SparkSession.builder()
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
      .appName("ctr fm")
      .getOrCreate()

    val sampleRate = 0.9
    var pathSep = Seq[String]()
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      pathSep = pathSep :+ date
      cal.add(Calendar.DATE, 1)
    }
    println("%s/{%s}".format(inpath, pathSep.mkString(",")))


    /*
    val model = FMWithSGD.train(svm, task = 1, numIterations = 10,
      stepSize = 0.1, miniBatchFraction = 1, dim = (true, true, 10),
      regParam = (0, 0, 0), initStd = 0.01)

    val model = FMWithLBFGS.train(svm, task = 1, numIterations = 100,
      numCorrections = 10, dim = (true, true, 20),
      regParam = (0, 0, 0), initStd = 0.1)
      */






    val rawData = MLUtils.loadLibSVMFile(ctx.sparkContext, "%s/{%s}".format(inpath, pathSep.mkString(",")))
    val totalNum = rawData.count()
    val svm = rawData.coalesce(math.max(200, 50))
      .randomSplit(Array(sampleRate, 1 - sampleRate), seed = new Date().getTime)
    val sample = svm(0)
      .filter(x => x.label > 0.01 || Random.nextInt(100) < 44)
      .cache()
    val k = 20

    println("sample count", sample.count(), sample.partitions.length)
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

    val fm1: FMModel = FMWithSGD.train(sample, task = 0, numIterations = 50,
      stepSize = 0.1, dim = (false, true, k), regParam = (0, 0.1, 0.01), initStd = 0.01)

    //stocGradAscent(svm.take(10000).toIterator, w.toArray, w2, k, 0.1)
    val n = sample.first().features.size
    //var w = Vectors.dense(Array.fill(n)(0d)).toSparse
    //val w2 = Array.fill(k)(Vectors.dense(Array.fill(n)(0.01d)).toSparse)
    //stocGradAscent(sample.take(1000).toIterator, w, w2, 0.1)
    //val (w, w2) = sparkStocGradAscent(ctx.sparkContext, sample, k, 50)


    val layers = Array[Int](n, 100, 1) // Note 2 neurons in the input layer
    val trainer = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setSeed(1234L)
      .setMaxIter(100)


    ctx.createDataFrame(sample).w

    trainer.fit(sample)






    println("testing...")
    val test = svm(1)
    val testResults = test.map{
      case LabeledPoint(label, features) =>
        val p = fm1.predict(features.toSparse)
        (p, label)
    }

    testResults.take(200).foreach(println)

    var test0 = 0
    var test1 = 0
    testResults
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

    val testSum = testResults.count()
    println("predict distribution %s %d(1) %d(0)".format(testSum, test1, test0))
    testResults
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
    val metrics = new BinaryClassificationMetrics(testResults.map(x => (x._1, x._2)))
    val auPRC = metrics.areaUnderPR
    //val roc = metrics.roc
    val auROC = metrics.areaUnderROC
    println("auPRC: %.10f, auROC: %.10f".format(auPRC, auROC))
  }

  def sigmoid(m: Double): Double = {
    1d / (1d + math.exp(-m))
  }

  def sparkStocGradAscent(sc: SparkContext, sample: RDD[LabeledPoint],
                          k: Int, iterNum: Int): (SparseVector, Array[SparseVector]) = {
    val tolerance = 1e-6
    val train = sample.cache()
    val m = train.count()
    val n = train.first().features.size
    var alpha = 0.1d

    var w = Vectors.dense(Array.fill(n)(0d)).toSparse
    val w2 = Array.fill(k)(Vectors.dense(Array.fill(n)(0.01d)).toSparse)

    var loss = 1d
    val nump = train.getNumPartitions
    println("partition num", nump)
    for (it <- 0 until iterNum) {
      val wb = sc.broadcast(w)
      val w2b = sc.broadcast(w2)
      val wrdd = train
        .mapPartitions {
          p =>
            Seq(stocGradAscent(p, wb.value, w2b.value, alpha)).iterator
        }
        .filter(_._1 > 0)
        .sortBy(_._1)
        .take(nump)

      val sum = wrdd.map(_._1).sum
      val newloss = sum / nump
      val stepRate = (loss - newloss) / loss
      loss = newloss

      val values = new Array[Double](n)
      wrdd.map(_._2)
        .foreach {
          x =>
            x.foreachActive {
              (i, v) =>
                values(i) = values(i) + v
            }
        }
      w = Vectors.dense(values.map(_/nump)).toSparse

      val m = Array.fill(k)(Array.fill(n)(0d))
      wrdd.map(_._3).foreach {
        v =>
          for (f <- 0 until k) {
            for (i <- 0 until n) {
              m(f)(i) = m(f)(i) + v(f)(i)
            }
          }
      }
      for (f <- 0 until k) {
        w2(f) = Vectors.dense(m(f).map(_/nump)).toSparse
      }

      println("iter: %d, loss: %.8f, stepRate: %.5f, %d".format(it, loss, stepRate, w.numActives))
    }

    train.unpersist()
    (w, w2)
  }

  def stocGradAscent(train: Iterator[LabeledPoint], w: SparseVector, w2: Array[SparseVector],
                     alpha: Double): (Double, SparseVector, Array[SparseVector]) = {
    var minloss = 1d
    val weights = w.toArray
    for (LabeledPoint(label, features) <- Random.shuffle(train)) {
      val hypothesis = predict(features.toSparse, weights, w2)
      val loss = hypothesis - label

      if (minloss > math.abs(loss)) {
        minloss = math.abs(loss)
      }

      features.foreachActive {
        (i, v) =>
          weights(i) = weights(i) - alpha * (loss * v + weights(i) * 0.2)
      }

      for (f <- w2.indices) {
        features.foreachActive {
          (i, v) =>
            val vec = w2(f).toArray
            vec(i) = vec(i) - alpha * loss * v * (multiply(features.toSparse, vec).toArray.sum - vec(i) * v)
            w2(f) = Vectors.dense(vec).toSparse
        }
      }
    }
    (minloss, Vectors.dense(weights).toSparse, w2)
  }

  def predict(features: SparseVector, w: SparseVector, w2: Array[SparseVector]): Double = {
    predict(features, w.toArray, w2)
  }

  def predict(features: SparseVector, w: Array[Double], w2: Array[SparseVector]): Double = {
    var pred = 0d
    pred += dot(features, w)
    if (w2.length > 0) {
      val sum = Array.fill(w2.length)(0.0)
      for (f <- w2.indices) {
        var sumSqr = 0.0
        features.foreachActive {
          case (i, v) =>
            val d = w2(f)(i)
            sum(f) += d
            sumSqr += d * d
        }
        pred += (sum(f) * sum(f) - sumSqr) * 0.5
      }
    }
    pred
  }

  private def dot(v1: SparseVector, v2: Array[Double]): Double = {
    var sum = 0d
    v1.foreachActive {
      (i, v) =>
        sum += v * v2(i)
    }
    sum
  }

  private def dot(v1: SparseVector, v2: Vector): Double = {
    var sum = 0d
    v1.foreachActive {
      (i, v) =>
        sum += v * v2(i)
    }
    sum
  }

  private def multiply(v1: SparseVector, v2: Vector): SparseVector = {
    var els = Seq[(Int, Double)]()
    v1.foreachActive {
      (i, v) =>
        els = els :+ (i, v2(i) * v)
    }
    Vectors.sparse(v1.size, els).toSparse
  }

  private def multiply(v1: SparseVector, vs: Array[SparseVector]): SparseVector = {
    //val els = new Array[(Int, Double)](v1.size)
    val vals = Array.fill(v1.size)(0d)
    vs.map {
      v =>
        multiply(v1, v)
    }
    .foreach {
      vec =>
        vec.foreachActive {
          (i, v) =>
            vals(i) += v
        }
    }
    Vectors.dense(vals).toSparse
  }

  private def multiply(v1: SparseVector, v2: Array[Double]): SparseVector = {
    var els = Seq[(Int, Double)]()
    v1.foreachActive {
      (i, v) =>
        els = els :+ (i, v2(i) * v)
    }
    Vectors.sparse(v1.size, els).toSparse
  }
}


