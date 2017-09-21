package com.cpc.spark.ml.ctrmodel.v2




import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.cpc.spark.common.{Utils => CUtils}
import com.cpc.spark.ml.common.{DspData, Utils}
import com.cpc.spark.ml.train.LRIRModel
import com.typesafe.config.ConfigFactory
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils


/**
  * Created by roydong on 06/07/2017.
  */
object LRTrain {

  def main(args: Array[String]): Unit = {
    if (args.length < 10) {
      System.err.println(
        s"""
           |Usage: CtrModel <mode:train/test[+ir]>
           |  <svmPath:string> <dayBefore:int> <day:int>
           |  <modelPath:string> <sampleRate:float> <PNRate:int>
           |  <IRBinNum:int> <LRFile:string> <IRFile:string>
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
    val ctx = model.initSpark("cpc ctr model %s [%s]".format(mode, modelPath))

    println("loading dsp data")
    val badmap = ctx.sparkContext.broadcast(DspData.getAdDict())

    val date = new SimpleDateFormat("yyyy-MM-dd-HH").format(new Date().getTime)
    val cal = Calendar.getInstance()
    cal.add(Calendar.DATE, -daybefore)
    var pathSep = Seq[String]()
    for (n <- 1 to days) {
      val date = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime)
      pathSep = pathSep :+ date
      cal.add(Calendar.DATE, 1)
    }

    println("loading sample data %s/{%s}".format(inpath, pathSep.mkString(",")))
    val rawData = MLUtils.loadLibSVMFile(ctx.sparkContext, "%s/{%s}".format(inpath, pathSep.mkString(",")))
    val totalNum = rawData.count()
    val svm = rawData.coalesce(totalNum.toInt / 20000)
      .mapPartitions {
        p =>
          p.map {
            x =>
              var els = Seq[(Int, Double)]()
              val admap = badmap.value
              var id = 0
              x.features.foreachActive {
                (i, v) =>
                  if (i < 3595) {
                    els = els :+ (i, v)
                  } else if (i == 3595) {
                    id = v.toInt
                  }
              }

              val ad = admap.getOrElse(id, null)
              if (ad != null) {
                els = els :+ (3595 + ad.adid, 1d)
                for (i <- 0 until 10) {
                  if (i < ad.titles.length) {
                    els = els :+ (80000 + i * 12000 + ad.titles(i), 1d)
                  }
                }

                val v = Vectors.sparse(80000 + 120000, els)
                LabeledPoint(x.label, v)
              } else {
                null
              }
          }
      }
      .filter(_ != null)
      .randomSplit(Array(0.9, 0.1), seed = new Date().getTime)

    val sample = svm(0).cache()
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

    sample.take(5).foreach(x => println(x.features))
    println("training...")
    model.run(sample, 0, 1e-6)
    model.saveHdfs(modelPath + "/" + date)
    sample.unpersist()
    println("done")

    val testSample = svm(1)
    println("testing...")
    model.test(testSample)
    model.printLrTestLog()
    val lrTestLog = model.getLrTestLog()
    println("done")

    var updateOnlineData = 0
    val lrfilepath = "/data/cpc/anal/model/logistic_%s.txt".format(date)
    model.saveText(lrfilepath)

    //满足条件的模型直接替换线上数据
    if (lrfile.length > 0 && model.getAuPRC() > 0.07 && model.getAuROC() > 0.80) {
      updateOnlineData += 1
    }

    var irError = 0d
    val irfilepath = "/data/cpc/anal/model/isotonic_%s.txt".format(date)
    println("start isotonic regression")
    irError = model.runIr(binNum, 0.9)
    model.saveIrHdfs(modelPath + "/" + date + "_ir")
    model.saveIrText(irfilepath)
    if (irfile.length > 0 && math.abs(irError) < 0.01) {
      updateOnlineData += 1
    }

    val irBinsLog = model.binsLog.mkString("\n")
    val conf = ConfigFactory.load()
    var result = "failed"
    var nodes = ""
    if (updateOnlineData == 2) {
      println("replace online data")
      nodes = Utils.updateOnlineData(lrfilepath, lrfile, conf)
      Utils.updateOnlineData(irfilepath, irfile, conf)
      result = "success"
    }

    val txt =
      """
        |date: %s
        |LRfile: %s
        |auPRC: %.6f need > 0.07
        |auROC: %.6f need > 0.80
        |IRError: %.6f need < |0.01|
        |
        |===========================
        |%s
        |
        |===========================
        |%s
        |
        |===========================
        |%s
        |
        """.stripMargin.format(date, lrfilepath, model.getAuPRC(), model.getAuROC(), irError, lrTestLog, irBinsLog, nodes)
    CUtils.sendMail(txt, "CTR model train " + result, Seq("cpc-rd@innotechx.com"))

    println("all done")
    model.stopSpark()
  }
}


