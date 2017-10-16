package com.cpc.spark.ml.antimodel.v1

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.ml.train.LRIRModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

import scala.util.Random

/**
  * Created by roydong on 06/07/2017.
  */
object AntispamModel {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: CtrModel <mode:train/test>
           |   <pnRate:int> <daybefore:int>
        """.stripMargin)
      System.exit(1)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
    // "train+ir" \
   // "/user/cpc/svmdata/v5" 10 10 \
   //   "/user/cpc/model/v5" \
    //  1 1 5000 "20170801.logistic" "20170801.isotonic"

    Logger.getRootLogger.setLevel(Level.WARN)
    val mode = args(0).trim
    val pnRate = args(1).toInt
    val daybefore = args(2).toInt
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)
    val toDate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val modelPath = "/user/cpc/antispam/v1/model"
    val model = new LRIRModel
    val ctx = model.initSpark("cpc antispam v1 model %s [%s]".format(mode, modelPath))


    var testSample: RDD[LabeledPoint] = null
    if (mode.startsWith("test")) {
      testSample = MLUtils.loadLibSVMFile(ctx.sparkContext, "/user/cpc/antispam/v1/svm/test".format(toDate))
      model.loadLRmodel(modelPath)
    } else {
      val svm = MLUtils.loadLibSVMFile(ctx.sparkContext, "/user/cpc/antispam/v1/svm/train/%s".format(toDate))
        //random pick 1/pnRate negative sample
        .filter(x => x.label > 0.01 || Random.nextInt(pnRate) == 0)
        .randomSplit(Array(1, 0), seed = new Date().getTime)
      val sample = svm(0).cache()
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

      sample.take(1).foreach(x => println(x.features))
      println("training...")
      model.run(sample, 0, 1e-8)
      model.saveHdfs(modelPath + "/" + toDate)
      sample.unpersist()
      println("done")
    }

    if (testSample == null) {
      testSample = MLUtils.loadLibSVMFile(ctx.sparkContext, "/user/cpc/antispam/v1/svm/test/%s".format(toDate))
    }
    println("testing...")
    model.test(testSample)
    model.printLrTestLog()
    println("done")
    model.stopSpark()
  }
}


