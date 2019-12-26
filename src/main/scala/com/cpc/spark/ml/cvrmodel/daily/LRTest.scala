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


    val modelPath="hdfs://emr-cluster/user/cpc/lrmodel/miread_lrmodeldata/2019-12-26-04-15"
    // model
    model.loadLRmodel(modelPath)
    print("\nweights bias = "+ model.getLRmodel().weights.apply(0))
    print("\nweights hour = "+ model.getLRmodel().weights.apply(18))
    print("\nweights sex = "+ model.getLRmodel().weights.apply(26))
    print("\nweights age = "+ model.getLRmodel().weights.apply(35))
    print("\nweights os = "+ model.getLRmodel().weights.apply(135))
    print("\nweights isp = "+ model.getLRmodel().weights.apply(144))
    print("\nweights network = "+ model.getLRmodel().weights.apply(165))
    print("\nweights cityid = "+ model.getLRmodel().weights.apply(174))
    print("\nweights mediaid = "+ model.getLRmodel().weights.apply(542))
    print("\nweights slotid = "+ model.getLRmodel().weights.apply(563))
    print("\nweights phone_level = "+ model.getLRmodel().weights.apply(1023))
    print("\nweights pagenum = "+ model.getLRmodel().weights.apply(1033))
    print("\nweights bookid = "+ model.getLRmodel().weights.apply(1133))
    print("\nweights adclass = "+ model.getLRmodel().weights.apply(1233))
    print("\nweights adtype = "+ model.getLRmodel().weights.apply(1319))
    print("\nweights adslot_type = "+ model.getLRmodel().weights.apply(1333))
    print("\nweights planid = "+ model.getLRmodel().weights.apply(1342))
    print("\nweights unitid = "+ model.getLRmodel().weights.apply(12193))
    print("\nweights ideaid = "+ model.getLRmodel().weights.apply(24231))


    println("\nmodelPath = " + modelPath)
//    model.loadLRmodel("hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata_7/qtt-bs-cvrparser4-daily_2019-09-04-18-50")
//    model.loadLRmodel("hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata_7/qtt-bs-cvrparser4-daily_2019-09-09-18-50")

    // generate feature vector manually.
    var els = Seq[(Int, Double)]()

    els = els :+ (0, 1d)  //bias
    els = els :+ (18, 1d) // hour
    els = els :+ (26, 1d) // sex
    els = els :+ (35, 1d) // age
    els = els :+ (135, 1d) // os
    els = els :+ (144, 1d) // isp
    els = els :+ (165, 1d) // network
    els = els :+ (174, 1d) // cityid
    els = els :+ (542, 1d) // mediaid
    els = els :+ (563, 1d) // slotid
    els = els :+ (1023, 1d) // phone_level
    els = els :+ (1033, 1d) // pagenum
    els = els :+ (1133, 1d) // bookid
    els = els :+ (1233, 1d) // adclass
    els = els :+ (1319, 1d) // adtype
    els = els :+ (1333, 1d) // adslot_type
    els = els :+ (1342, 1d) // planid
    els = els :+ (12193, 1d) // unitid
    els = els :+ (24231, 1d) // ideaid


//    // hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata_7/qtt-bs-cvrparser4-daily_2019-09-09-18-50
//    val mediaid=39
//    val planid=15409
//    val unitid=17374
//    val ideaid=40041
//    val slotid=452
//    val adclass=100
//    val cityid=367
//
//
//    val size=1+24+9+100+10+20+10+cityid+1+mediaid+1+slotid+1+10+100+100+adclass+1+16+10+planid+1+unitid+1+ideaid+1+1001
//    println("size = " + size)

    println("weights = " + model.getLRmodel().weights.toArray)

    val vectorToPredict : Vector = Vectors.sparse(model.getLRmodel().weights.size, els)

    println("-- prediction result: %s --"
      .format(
        model
          .getLRmodel
          .predict(vectorToPredict)
      )
    )

  }
}