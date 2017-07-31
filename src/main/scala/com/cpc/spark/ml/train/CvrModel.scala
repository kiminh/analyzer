package com.cpc.spark.ml.train

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

import scala.sys.process._
import scala.util.Random

/**
  * Created by roydong on 06/07/2017.
  */
object CvrModel {

  def main(args: Array[String]): Unit = {
    if (args.length < 9) {
      System.err.println(
        s"""
           |Usage: CtrModel <mode:train/test[+ir]>
           |  <svmPath:string> <dayBefore:int> <day:int>
           |  <modelPath:string> <sampleRate:float> <PNRate:int>
           |  <IRBinNum:int> <lrfile:string> <irfile:string>
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
    val lrfile = args(8)
    val irfile = args(9)

    val model = new LRIRModel
    val ctx = model.initSpark("cpc cvr model %s [%s]".format(mode, modelPath))

    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    val date = new SimpleDateFormat("yyyy-MM-dd-HH").format(new Date().getTime)
    val yesterday = fmt.format(new Date().getTime - 3600L * 24000L)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)

    var pathSep = Seq[String]()
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      pathSep = pathSep :+ date
      cal.add(Calendar.DATE, 1)
    }
    println("%s/{%s}".format(inpath, pathSep.mkString(",")))
    val svm = MLUtils.loadLibSVMFile(ctx.sparkContext, "%s/{%s}".format(inpath, pathSep.mkString(",")))
      //random pick 1/pnRate negative sample
      .filter(x => x.label > 0.01 || Random.nextInt(pnRate) == 0)
      .randomSplit(Array(sampleRate, 1 - sampleRate), seed = new Date().getTime)
    val testSample = svm(1)

    if (mode.startsWith("test")) {
      model.loadLRmodel(modelPath)
    } else {
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

      //sample.take(1).foreach(x => println(x.features))
      println("training...")
      model.run(sample, 0, 0)
      model.saveHdfs(modelPath + "/" + date)
      sample.unpersist()
      println("done")
    }

    println("testing...")
    model.test(testSample)
    model.printLrTestLog()
    println("done")
    if (mode.startsWith("train")) {
      val filepath = "/home/cpc/anal/model/cvr_logistic_%s.txt".format(date)
      model.saveText(filepath)

      //满足条件的模型直接替换线上数据
      if (lrfile.length > 0 && model.getAuPRC() > 0.3 && model.getAuROC() > 0.75) {
        println("replace lr online data")
        val ret  = s"cp $filepath                /home/work/ml/model/$lrfile" !
        val ret1 = s"scp $filepath work@cpc-bj01:/home/work/ml/model/$lrfile" !
        val ret2 = s"scp $filepath work@cpc-bj05:/home/work/ml/model/$lrfile" !
        val ret3 = s"scp $filepath work@cpc-bj08:/home/work/ml/model/$lrfile" !
        val ret4 = s"scp $filepath work@cpc-bj09:/home/work/ml/model/$lrfile" !
      }
    }

    if (mode.endsWith("+ir")) {
      println("start isotonic regression")
      val meanError = model.runIr(binNum, 0.9)
      val filepath = "/home/cpc/anal/model/cvr_isotonic_%s.txt".format(date)
      model.saveIrText(filepath)
      if (irfile.length > 0 && math.abs(meanError) < 0.001) {
        println("replace ir online data")
        val ret = s"cp $filepath                 /home/work/ml/model/$irfile" !
        val ret1 = s"scp $filepath work@cpc-bj01:/home/work/ml/model/$irfile" !
        val ret2 = s"scp $filepath work@cpc-bj05:/home/work/ml/model/$irfile" !
        val ret3 = s"scp $filepath work@cpc-bj08:/home/work/ml/model/$irfile" !
        val ret4 = s"scp $filepath work@cpc-bj09:/home/work/ml/model/$irfile" !
      }
    }

    println("all done")
    model.stopSpark()
  }
}
