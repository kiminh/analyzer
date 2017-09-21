package com.cpc.spark.ml.ctrmodel.v2
import java.text.SimpleDateFormat
import java.util.{Calendar, Date, Properties}

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
object LRTest {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println(
        s"""
           |Usage: CtrModel <svmPath:string> <dayBefore:int> <day:int>
           |  <modelPath:string>
        """.stripMargin)
      System.exit(1)
    }

    Logger.getRootLogger.setLevel(Level.WARN)
    val inpath = args(0).trim
    val daybefore = args(1).toInt
    val days = args(2).toInt
    val modelPath = args(3).trim

    val model = new LRIRModel
    val ctx = model.initSpark("cpc ctr model test [%s]".format(modelPath))

    println("loading dsp data")
    val badmap = ctx.sparkContext.broadcast(DspData.getAdDict())
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
    val testSample = rawData
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

    testSample.take(5).foreach(x => println(x.features))
    model.loadLRmodel(modelPath)
    println("testing...")
    model.test(testSample)
    model.printLrTestLog()
    println("done")

    model.stopSpark()
  }
}
