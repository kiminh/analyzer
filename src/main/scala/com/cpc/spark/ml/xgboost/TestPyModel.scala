package com.cpc.spark.ml.xgboost

import java.io.{FileInputStream, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.Date

import ml.dmlc.xgboost4j.scala.XGBoost
import ml.dmlc.xgboost4j.scala.spark.XGBoostEstimator
import mlmodel.mlmodel.{Dict, IRModel, Pack}
import org.apache.spark.mllib.regression.{IsotonicRegression, IsotonicRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import com.cpc.spark.ml.common.Utils
import com.typesafe.config.ConfigFactory

import scala.io.Source
import sys.process._

/**
  * Created by roydong on 19/03/2018.
  */
object TestPyModel {

  var spark: SparkSession = null;
  private var irmodel: IsotonicRegressionModel = _
  private var irError = 0d

  def main(args: Array[String]): Unit = {
    spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("test xgboost pymodel and run IR")
      .enableHiveSupport()
      .getOrCreate()
    val filetime = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)

    val results = spark.read.parquet("/user/cpc/xgboost_test_results")
      .rdd
      .map {
        x =>
          val p = x.getDouble(0)
          val label = x.getDouble(1)
          (p, label)
      }

    runIr(results, 1000, 0.95)
    val ir = IRModel(
      boundaries = irmodel.boundaries.toSeq,
      predictions = irmodel.predictions.toSeq,
      meanSquareError = irError * irError
    )

    irmodel.save(spark.sparkContext, "/user/cpc/xgboost_ir/"+filetime)

    val treeLimit = Source.fromFile("/tmp/xgboost_best_ntree_limit.txt").mkString.toInt
    println("tree limit", treeLimit)
    val mlm = Pack.parseFrom(new FileInputStream("/tmp/xgboost.mlm"))


    val prefix = "%s-%s".format(args(0), args(1))
    val filename = "/home/cpc/anal/xgboost_model/%s-%s".format(prefix, filetime)
    println(filename)
    val pack = Pack(
      createTime = mlm.createTime,
      lr = mlm.lr,
      ir = Option(ir),
      dict = mlm.dict,
      gbmTreeLimit = treeLimit,
      gbmfile = s"/home/work/mlcpp/data/$prefix.gbm"
    )
    pack.writeTo(new FileOutputStream(s"$filename.mlm"))
    val cmd = s"cp -f /tmp/xgboost.gbm $filename.gbm"
    val ret = cmd !


    if (args(2).toInt == 1) {
      val conf = ConfigFactory.load()
      println(Utils.updateMlcppOnlineData(filename+".gbm", s"/home/work/mlcpp/data/$prefix.gbm", conf))
      println(Utils.updateMlcppOnlineData(filename+".mlm", s"/home/work/mlcpp/data/$prefix.mlm", conf))
    }
  }

  def getIntDict(in: String, out: String): Map[Int, Int] = {
    val dict = mutable.Map[Int, Int]()
    spark.read.parquet("/user/cpc/xgboost_dict/%s-%s".format(in, out))
      .rdd
      .toLocalIterator
      .foreach {
        x =>
          val k = x.getInt(0)
          val v = x.getInt(1)
          dict.update(k, v)
      }
    dict.toMap
  }

  def getAdslotDict(in: String, out: String): Map[Int, Int] = {
    val dict = mutable.Map[Int, Int]()
    spark.read.parquet("/user/cpc/xgboost_dict/%s-%s".format(in, out))
      .rdd
      .toLocalIterator
      .foreach {
        x =>
          val k = x.getString(0).toInt
          val v = x.getInt(1)
          dict.update(k, v)
      }
    dict.toMap
  }

  def getStringDict(in: String, out: String): Map[String, Int] = {
    val dict = mutable.Map[String, Int]()
    spark.read.parquet("/user/cpc/xgboost_dict/%s-%s".format(in, out))
      .rdd
      .toLocalIterator
      .foreach {
        x =>
          val k = x.getString(0)
          val v = x.getInt(1)
          dict.update(k, v)
      }
    dict.toMap
  }

  def runIr(results: RDD[(Double, Double)], binNum: Int, rate: Double): Double = {
    val sample = results.randomSplit(Array(rate, 1 - rate), seed = new Date().getTime)
    val bins = binData(sample(0), binNum)
    val sc = spark.sparkContext
    val ir = new IsotonicRegression().setIsotonic(true).run(sc.parallelize(bins.map(x => (x._1, x._3, 1d))))    // 真实值y轴，预测均值x轴   样本量就是桶的个数
    val sum = sample(1) //在测试数据上计算误差
      .map(x => (x._2, ir.predict(x._1))) //(click, calibrate ctr)
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    irError = (sum._2 - sum._1) / sum._1 //误差比
    irmodel = ir
    irError
  }

  private def binData(sample: RDD[(Double, Double)], binNum: Int): Seq[(Double, Double, Double, Double)] = {
    val binSize = sample.count().toInt / binNum  //每个桶的容量
    var bins = Seq[(Double, Double, Double, Double)]()
    var click = 0d  //正例数
    var pv = 0d  //总数或展示数
    var pSum = 0d  //预测值的累加
    var pMin = 1d  // 最小的预测值
    var pMax = 0d  // 最大的预测值
    var n = 0  //控制打印
    sample.sortByKey()  //(p, label)按照XGB预测值升序排序
      .toLocalIterator
      .foreach {
        x =>
          pSum = pSum + x._1
          if (x._1 < pMin) {
            pMin = x._1
          }
          if (x._1 > pMax) {
            pMax = x._1
          }
          if (x._2 > 0.01) {
            click = click + 1
          }
          pv = pv + 1
          if (pv >= binSize) {  //如果超过通的容量，就换下一个桶
            val ctr = click / pv    //  点击/展示
            bins = bins :+ (ctr, pMin, pSum / pv, pMax)  // 真实值，最小值，预测均值，最大值
            n = n + 1
            if (n > binNum - 20) {
              val logStr = "bin %d: %.6f(%d/%d) %.6f %.6f %.6f".format(
                n, ctr, click.toInt, pv.toInt, pMin, pSum / pv, pMax) //桶号：真实ctr（点击/展示），最小值，预测均值，最大值

              println(logStr)
            }

            click = 0d
            pv = 0d
            pSum = 0d
            pMin = 1d
            pMax = 0d
          }
      }
    bins
  }

}
