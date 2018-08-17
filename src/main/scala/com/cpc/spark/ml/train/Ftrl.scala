package com.cpc.spark.ml.train

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer
import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.SparkSession


class Ftrl {

  var alpha: Double = 0.01
  var beta: Double = 1.0
  var L1: Double = 0.0
  var L2: Double = 0.0
  //  var n, z, w = new Array[Double](200 * math.pow(2, 7).toInt)
  var n, z, w = new Array[Double](500000)

  def toJsonString(): String = {
    var json = new JSONObject()
    json.put("alpha", alpha)
    json.put("beta", beta)
    json.put("L1", L1)
    json.put("L2", L2)
    json.put("n", n.mkString(" "))
    json.put("z", z.mkString(" "))
    json.put("w", w.mkString(" "))
    // println(json.toJSONString)
    json.toJSONString()
  }

  def print() = {
    println("n:")
    println(n.mkString(" "))
    println("z:")
    println(z.mkString(" "))
    println("w:")
    println(w.mkString(" "))
    println("alpha:")
    println(alpha)
    println("beta:")
    println(beta)
    println("L1:")
    println(L1)
    println("L2:")
    println(L2)

  }

  def predict(x: Array[Int]): Double = {
    var wTx = 0.0

    x foreach { x =>
      val sign = if (z(x) < 0) -1.0 else 1.0

      if (sign * z(x) <= L1)
        w(x) = 0.0
      else
        w(x) = (sign * L1 - z(x)) / ((beta + math.sqrt(n(x))) / alpha + L2)

      wTx = wTx + w(x)
    }
    return 1.0 / (1.0 + math.exp(-wTx))
  }

  def update(x: Array[Int], p: Double, y: Double): Unit = {
    val g = p - y

    x foreach { x =>
      val sigma = (math.sqrt(n(x) + g * g) - math.sqrt(n(x))) / alpha
      z(x) = z(x) + g - sigma * w(x)
      n(x) = n(x) + g * g
    }

  }

  def train(spark:  SparkSession, data: RDD[LabeledPoint]): Unit = {

    val res = data.repartition(1).collect()
    var listBuffer = new ListBuffer[(Double, Double)]
    for (p <- res) {
      val x = p.features.toSparse.indices
      val pre = predict(x)
      val label = p.label
      update(x, pre, p.label)
      listBuffer.append((pre, p.label))
    }

    val rdd = spark.sparkContext.parallelize[(Double, Double)](listBuffer)
    val metrics = new BinaryClassificationMetrics(rdd)
    val auROC = metrics.areaUnderROC
    println(s"auc=$auROC")
  }


}
