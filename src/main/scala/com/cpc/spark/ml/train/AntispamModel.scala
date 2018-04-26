package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.ml.common.Utils
import com.typesafe.config.ConfigFactory
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
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: CtrModel <mode:train/test>
           |  <svmPath:string> <dayBefore:int> <day:int>
           |  <modelPath:string> <sampleRate:float> <PNRate:int>
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
    val inpath = args(1).trim
    /*val daybefore = args(2).toInt
    val days = args(3).toInt*/
    val modelPath = args(2).trim
   /* val sampleRate = args(3).toFloat
    val pnRate = args(4).toInt*/
    val daybefore = args(3).toInt
    /*val binNum = args(7).toInt
    val lrfile = args(8)
    val irfile = args(9)*/
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)

    val toDate = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)

    val model = new LRIRModel
    val ctx = model.initSpark("cpc antispam model %s [%s]".format(mode, modelPath))

    /* val fmt = new SimpleDateFormat("yyyy-MM-dd")
     val date = new SimpleDateFormat("yyyy-MM-dd-HH").format(new Date().getTime)
     val yesterday = fmt.format(new Date().getTime - 3600L * 24000L)*/
    /*val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)

    var pathSep = Seq[String]()
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      pathSep = pathSep :+ date
      cal.add(Calendar.DATE, 1)
    }*/
   // println("%s/{%s}".format(inpath, pathSep.mkString(",")))

    var testSample: RDD[LabeledPoint] = null
    if (mode.startsWith("test")) {
      testSample = MLUtils.loadLibSVMFile(ctx.sparkContext, "%s/_full/%s".format(inpath, toDate))
      model.loadLRmodel(modelPath)
    } else {
      val svm = MLUtils.loadLibSVMFile(ctx.sparkContext, "%s/%s".format(inpath, toDate))
        //random pick 1/pnRate negative sample
        //.filter(x => x.label > 0.01 || Random.nextInt(100) < 100)
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
      model.run(sample, 0, 0)
      model.saveHdfs(modelPath + "/" + toDate)
      sample.unpersist()
      println("done")
    }

    if (testSample == null) {
      testSample = MLUtils.loadLibSVMFile(ctx.sparkContext, "%s_full/%s".format(inpath, toDate))
    }
    println("testing...")
    model.test(testSample)
    model.printLrTestLog()
    println("done")
    model.stopSpark()
  }
}


