package com.cpc.spark.ml.calibration

import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class TestHourlyCalibration extends FlatSpec with Matchers with BeforeAndAfter {

  private val master = "local"
  private val appName = "unit-test-spark"

  private var sc: SparkContext = _

  before {
    val conf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sc = new SparkContext(conf)
  }

  "binIterable" should "generate bins from unordered tuples" in {
    val input = List((1d, 1d), (0.5d, 0d), (0.8d, 1d), (0.2d, 0d))
    val result =  HourlyCalibration.binIterable(input, 1, 10)
    result should be (List((0d, 0.2d, 1d), (0d, 0.5d, 1d), (1d, 0.8d, 1d), (1d, 1d, 1d)))
  }

  it should "merged and generate bins" in {
    val input = List((1d, 1d), (0.5d, 0d), (0.8d, 1d), (0.2d, 0d))
    val result =  HourlyCalibration.binIterable(input, 2, 4)
    result should be (List((0d, 0.35d, 1d), (1d, 0.9d, 1d)))
  }

  it should "return a single bin when too many bins are needed from minBin" in {
    val input = List((1d, 1d), (0.5d, 0d), (0.8d, 1d), (0.2d, 0d))
    var result =  HourlyCalibration.binIterable(input, 5, 2)
    result should be (List((0.5d, 0.625d, 1d)))
    result =  HourlyCalibration.binIterable(input, 3, 1)
    result should be (List((0.5d, 0.625d, 1d)))
  }

  "unionLogToConfig" should "generate correct config from union log rdd" in {
    val rdd = sc.parallelize(Seq(
      Row(1, (1d * 1e6).toLong, "", "v1"),
      Row(0, (0.5d * 1e6).toLong, "", "v1"),
      Row(1, (0.8d * 1e6).toLong, "", "v1"),
      Row(0, (0.2d * 1e6).toLong, "", "v1")))
    var result = HourlyCalibration.unionLogToConfig(rdd, sc, 1, false, 2, 4)
    result.size should be (1)
    result.head.name should be ("v1")
    result.head.ir.get.boundaries should be (Seq(0.35, 0.9))
    result.head.ir.get.predictions should be (Seq(0.0, 1.0))
  }

  it should "generate isotonic model" in {
    val rdd = sc.parallelize(Seq(
      Row(1, (1d * 1e6).toLong, "", "v1"),
      Row(0, (0.5d * 1e6).toLong, "", "v1"),
      Row(1, (0.8d * 1e6).toLong, "", "v1"),
      Row(0, (0.9d * 1e6).toLong, "", "v1"),
      Row(0, (0.2d * 1e6).toLong, "", "v1"),
      Row(1, (0.3d * 1e6).toLong, "", "v1")))

    val result = HourlyCalibration.unionLogToConfig(rdd, sc, 1, false, 3, 4)
    result.size should be (1)
    result.head.name should be ("v1")
    result.head.ir.get.boundaries.toList.head should be (0.333333333 +- 1e-4)
    result.head.ir.get.boundaries.toList(1) should be (0.9 +- 1e-4)
    result.head.ir.get.predictions.toList.head should be (0.333333333 +- 1e-4)
    result.head.ir.get.predictions.toList(1) should be (0.6666666 +- 1e-4)
  }

  after {
    if (sc != null) {
      sc.stop()
    }
  }
}