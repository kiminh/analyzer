package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.linalg.{DenseMatrix, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import com.intel.imllib.fm.regression._

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
    val svm = rawData.coalesce(totalNum.toInt / 50000)
      .randomSplit(Array(sampleRate, 1 - sampleRate), seed = new Date().getTime)
    val sample = svm(0)
      .filter(x => x.label > 0.01 || Random.nextInt(100) < 44)
      .cache()

    val n = sample.first().features.size
    val k = 8
    val w = Vectors.dense(Array.fill(n)(0d)).toSparse
    val w2 = Array.fill(k)(Vectors.dense(Array.fill(n)(0.1d)).toSparse)

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
    val fm1: FMModel = FMWithSGD.train(sample, task = 1, numIterations
      = 100, stepSize = 0.1, dim = (false, true, k), regParam = (0, 0.0, 0.01), initStd = 0.01)


    println("testing...")

    //stocGradAscent(svm.take(10000).toIterator, w.toArray, w2, k, 0.1)


    //val w = sparkStocGradAscent(ctx.sparkContext, svm, 1, 200)

    val test = svm(1)
    val testResults = test.map{
      case LabeledPoint(label, features) =>
        val p = fm1.predict(features)
        (p, label)
    }

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
    val metrics = new BinaryClassificationMetrics(testResults)
    val auPRC = metrics.areaUnderPR
    //val roc = metrics.roc
    val auROC = metrics.areaUnderROC
    println("auPRC: %.10f, auROC: %.10f".format(auPRC, auROC))
  }

  def sigmoid(m: Double): Double = {
    1d / (1d + math.exp(-m))
  }

  def sparkStocGradAscent(sc: SparkContext, sample: RDD[LabeledPoint], k: Int, iterNum: Int): Vector = {
    val tolerance = 1e-6
    val train = sample.cache()
    val m = train.count()
    val n = train.first().features.size
    var alpha = 0.1d

    var w = Vectors.dense(Array.fill(n)(0d)).toSparse
    var w2 = Array.fill(k)(w)

    var loss = 1d
    var stopNum = 0
    val nump = train.getNumPartitions
    val nodeNum = m / nump
    println("partition num", nump, nodeNum)
    for (it <- 0 until iterNum) {
      val wb = sc.broadcast(w)
      val w2b = sc.broadcast(w2)
      val wrdd = train
        .mapPartitions {
          p =>
            val w = wb.value.toArray
            val w2 = w2b.value
            Seq(stocGradAscent(p, w, w2, k, alpha)).iterator
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
      w2 = wrdd.take(1).map(_._3).head
      wb.destroy()
      println("iter: %d, loss: %.8f, stepRate: %.5f, %d".format(it, loss, stepRate, w.numActives))
    }

    train.unpersist()
    println("w:", w.toSparse.numActives)
    w
  }

  def stocGradAscent(train: Iterator[LabeledPoint], w: Array[Double], w2: Array[SparseVector],
                     k: Int, alpha: Double): (Double, SparseVector, Array[SparseVector]) = {
    var minloss = 1d
    for (LabeledPoint(label, features) <- Random.shuffle(train)) {
      val w22 = new Array[SparseVector](w2.length)
      var y2 = 0d
      if (k > 0) {
        for (i <- w2.indices) {
          w22(i) = multiply(w2(i), w2(i))
        }

        val inter1 = multiply(features.toSparse, w2)
        val inter2 = multiply(multiply(features.toSparse, features), w22)
        multiply(inter1, inter1).foreachActive {
          (i, v) =>
            y2 += v - inter2(i)
            println(y2)
        }
      }
      val y1 = dot(features.toSparse, w)
      val hypothesis = sigmoid(y1 + y2 / 2)
      val loss = label - hypothesis
      if (minloss > math.abs(loss)) {
        minloss = math.abs(loss)
      }
      features.foreachActive {
        (i, v) =>
          w(i) = w(i) + alpha * loss * v
          for (f <- 0 until k) {
            val vec = w2(f).toArray
            vec(i) = vec(i) + alpha * loss * v * (multiply(features.toSparse, vec).toArray.sum - vec(i) * v)
            w2(f) = Vectors.dense(vec).toSparse
          }
      }

      println("--", minloss, loss, y2)
    }
    (minloss, Vectors.dense(w).toSparse, w2)
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


