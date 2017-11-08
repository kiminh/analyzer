package com.cpc.spark.ml.antimodel.v1

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.ml.common.Utils
import com.cpc.spark.ml.train.LRIRModel
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import com.cpc.spark.common.{Utils => CUtils}
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
   /* val mode = args(0).trim
    val pnRate = args(1).toInt
    val daybefore = args(2).toInt
    val lrfile = args(3)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)*/
    val mode = args(0).trim
    val daybefore = args(1).toInt
    val days = args(2).toInt
    val pnRate = args(3).toInt
  //  val sampleRate = args(5).toFloat
   //
  //  val binNum = args(7).toInt
    val   lrfile = args(4)

    val fmt = new SimpleDateFormat("yyyy-MM-dd")
    val date = new SimpleDateFormat("yyyy-MM-dd-HH").format(new Date().getTime)
    val yesterday = fmt.format(new Date().getTime - 3600L * 24000L)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)

    var pathSep = Seq[String]()
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      pathSep = pathSep :+ date
      cal.add(Calendar.DATE, -1)
    }
    println("/user/cpc/antispam/v1/svm/train/{%s}".format(pathSep.mkString(",")))


    val modelPath = "/user/cpc/antispam/v1/model"
    val model = new LRIRModel
    val ctx = model.initSpark("cpc antispam v1 model %s [%s]".format(mode, modelPath))

    var testSample: RDD[LabeledPoint] = null
    if (mode.startsWith("test")) {
      testSample = MLUtils.loadLibSVMFile(ctx.sparkContext, "/user/cpc/antispam/v1/svm/test/{%s}".format(pathSep.mkString(",")))
      model.loadLRmodel(modelPath)
    } else {
      val svm = MLUtils.loadLibSVMFile(ctx.sparkContext, "/user/cpc/antispam/v1/svm/train/{%s}".format(pathSep.mkString(",")))
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
      model.saveHdfs(modelPath + "/" + date)
      sample.unpersist()
      println("done")
    }

    if (testSample == null) {
      testSample = MLUtils.loadLibSVMFile(ctx.sparkContext, "/user/cpc/antispam/v1/svm/test/{%s}".format(pathSep.mkString(",")))
    }
    println("testing...")
    model.test(testSample)
    model.printLrTestLog()
    val lrTestLog = model.getLrTestLog()
    println("done")

    var updateOnlineData = 0
    val lrfilepath = "/data/cpc/anal/model/antispam_logistic_v1_%s.txt".format(date)
    if (mode.startsWith("train")) {
      model.saveText(lrfilepath)

      //满足条件的模型直接替换线上数据
      if (lrfile.length > 0 && model.getAuPRC() > 0.4 && model.getAuROC() > 0.74) {
        updateOnlineData += 1
      }
    }
    val conf = ConfigFactory.load()
    var result = "failed"
    var nodes = ""
    if (updateOnlineData == 1) {
      println("replace online data")
      nodes = Utils.updateOnlineData(lrfilepath, lrfile, conf)
      //Utils.updateOnlineData(irfilepath, irfile, conf)
      result = "success"
    }

    val txt =
      """
        |date: %s
        |LRfile: %s
        |auPRC: %.6f need > 0.4
        |auROC: %.6f need > 0.74
        |===========================
        |%s
        |
        |===========================
        |%s
        """.stripMargin.format(date, lrfilepath, model.getAuPRC(), model.getAuROC(),lrTestLog, nodes)
    CUtils.sendMail(txt, "antispam model train " + result, Seq("cpc-rd@innotechx.com"))

    model.stopSpark()
  }
}


