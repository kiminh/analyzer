package com.cpc.spark.ml.ctrmodel.hourly

import java.util.Calendar

import com.cpc.spark.ml.ctrmodel.hourly.LRTrain.model
import com.cpc.spark.ml.train.LRIRModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @author fym
  * @version created: 2019-05-13 18:06
  * @desc
  */
object LRTest {
  private var trainLog = Seq[String]()
  private val model = new LRIRModel

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val spark: SparkSession = model
      .initSpark("[cpc-model] lr-model test")

    // model
    model.loadLRmodel("hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata/2019-05-13-15-05")

    // generate feature vector manually.
    var els = Seq[(Int, Double)]()

    els = els :+ (2, 1d)
    els = els :+ (21, 1d) // hour
    els = els :+ (31, 1d) // sex
    els = els :+ (40, 1d) // age
    els = els :+ (141, 1d) // os
    els = els :+ (150, 1d) // isp
    els = els :+ (171, 1d) // network
    els = els :+ (180, 1d) // cityid
    els = els :+ (549, 1d) // mediaid
    els = els :+ (923, 1d) // slotid
    els = els :+ (1871, 1d) // phone_level
    els = els :+ (1877, 1d) // pagenum
    els = els :+ (1977, 1d) // bookid
    els = els :+ (2129, 1d) // adclass
    els = els :+ (2175, 1d) // adtype
    els = els :+ (2183, 1d) // adslot_type
    els = els :+ (2192, 1d) // planid
    els = els :+ (7349, 1d) // unitid
    els = els :+ (14124, 1d) // ideaid

    var i = 0

    i += 7
    i += 24
    i += 9
    i += 100
    i += 10
    i += 20
    i += 10
    i += 367 + 1 // cityid
    i += 371 + 1 // mediaid
    i += 946 + 1 // slotid
    i += 10
    i += 100
    i += 100
    i += 94 + 1 // adclass
    i += 10
    i += 10
    i += 5156 + 1 // planid
    i += 6774 + 1 // unitid
    i += 16927 + 1 // ideaid

    val vectorToPredict : Vector = Vectors.sparse(i, els)

    println("-- prediction result: %s --"
      .format(
        model
          .getLRmodel
          .predict(vectorToPredict)
      )
    )

  }
}
