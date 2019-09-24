package com.cpc.spark.ml.cvrmodel.daily

import java.util.Calendar

import com.cpc.spark.ml.ctrmodel.hourly.LRTrain.model
import com.cpc.spark.ml.train.LRIRModel
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * @author fym
  * @version created: 2019-05-28 10:53
  * @desc
  */
object LRTest {
  private var trainLog = Seq[String]()
  private val model = new LRIRModel

  def main(args: Array[String]): Unit = {

    Logger.getRootLogger.setLevel(Level.WARN)

    val spark: SparkSession = model
      .initSpark("[cpc-model] lr-model test")


    val modelPath="hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata/2019-09-23-16-07"
    // model
    model.loadLRmodel(modelPath)
    println("modelPath = " + modelPath)
//    model.loadLRmodel("hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata_7/qtt-bs-cvrparser4-daily_2019-09-04-18-50")
//    model.loadLRmodel("hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata_7/qtt-bs-cvrparser4-daily_2019-09-09-18-50")

    // generate feature vector manually.
    var els = Seq[(Int, Double)]()

    els = els :+ (0, 1d)  //bias
    els = els :+ (19, 1d) // hour
    els = els :+ (25, 1d) // sex
    els = els :+ (34, 1d) // age
    els = els :+ (135, 1d) // os
    els = els :+ (147, 1d) // isp
    els = els :+ (165, 1d) // network
    els = els :+ (537, 1d) // cityid
    els = els :+ (542, 1d) // mediaid
    els = els :+ (580, 1d) // slotid
    els = els :+ (1028, 1d) // phone_level
    els = els :+ (1034, 1d) // pagenum
    els = els :+ (1134, 1d) // bookid
    els = els :+ (1234, 1d) // adclass
    els = els :+ (1335, 1d) // adtype
    els = els :+ (1351, 1d) // adslot_type
    els = els :+ (1360, 1d) // planid
    els = els :+ (15892, 1d) // unitid
    els = els :+ (32213, 1d) // ideaid


    // hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata_7/qtt-bs-cvrparser4-daily_2019-09-09-18-50
    val mediaid=37
    val planid=14531
    val unitid=16320
    val ideaid=37990
    val slotid=443
    val adclass=99
    val cityid=367


    val size=1+24+9+100+10+20+10+cityid+1+mediaid+1+slotid+1+10+100+100+adclass+1+16+10+planid+1+unitid+1+ideaid+1+1001
    println("size = " + size)

    val vectorToPredict : Vector = Vectors.sparse(size, els)

    println("-- prediction result: %s --"
      .format(
        model
          .getLRmodel
          .predict(vectorToPredict)
      )
    )

  }
}