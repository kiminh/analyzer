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


    val modelPath="hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata/2019-09-24-23-31"
    // model
    model.loadLRmodel(modelPath)
    print("weights bias = "+ model.getLRmodel().weights.apply(0))
    print("weights hour = "+ model.getLRmodel().weights.apply(20))
    print("weights sex = "+ model.getLRmodel().weights.apply(25))
    print("weights age = "+ model.getLRmodel().weights.apply(34))
    print("weights os = "+ model.getLRmodel().weights.apply(135))
    print("weights isp = "+ model.getLRmodel().weights.apply(144))
    print("weights network = "+ model.getLRmodel().weights.apply(165))
    print("weights cityid = "+ model.getLRmodel().weights.apply(174))
    print("weights mediaid = "+ model.getLRmodel().weights.apply(542))
    print("weights slotid = "+ model.getLRmodel().weights.apply(582))
    print("weights phone_level = "+ model.getLRmodel().weights.apply(1039))
    print("weights pagenum = "+ model.getLRmodel().weights.apply(1045))
    print("weights bookid = "+ model.getLRmodel().weights.apply(1145))
    print("weights adclass = "+ model.getLRmodel().weights.apply(1245))
    print("weights adtype = "+ model.getLRmodel().weights.apply(1350))
    print("weights adslot_type = "+ model.getLRmodel().weights.apply(1363))
    print("weights planid = "+ model.getLRmodel().weights.apply(1372))
    print("weights unitid = "+ model.getLRmodel().weights.apply(16782))
    print("weights ideaid = "+ model.getLRmodel().weights.apply(34157))


    println("modelPath = " + modelPath)
//    model.loadLRmodel("hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata_7/qtt-bs-cvrparser4-daily_2019-09-04-18-50")
//    model.loadLRmodel("hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata_7/qtt-bs-cvrparser4-daily_2019-09-09-18-50")

    // generate feature vector manually.
    var els = Seq[(Int, Double)]()

    els = els :+ (0, 1d)  //bias
    els = els :+ (20, 1d) // hour
    els = els :+ (25, 1d) // sex
    els = els :+ (34, 1d) // age
    els = els :+ (135, 1d) // os
    els = els :+ (144, 1d) // isp
    els = els :+ (165, 1d) // network
    els = els :+ (174, 1d) // cityid
    els = els :+ (542, 1d) // mediaid
    els = els :+ (582, 1d) // slotid
    els = els :+ (1039, 1d) // phone_level
    els = els :+ (1045, 1d) // pagenum
    els = els :+ (1145, 1d) // bookid
    els = els :+ (1245, 1d) // adclass
    els = els :+ (1350, 1d) // adtype
    els = els :+ (1363, 1d) // adslot_type
    els = els :+ (1372, 1d) // planid
    els = els :+ (16782, 1d) // unitid
    els = els :+ (34157, 1d) // ideaid


    // hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata_7/qtt-bs-cvrparser4-daily_2019-09-09-18-50
    val mediaid=39
    val planid=15409
    val unitid=17374
    val ideaid=40041
    val slotid=452
    val adclass=100
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