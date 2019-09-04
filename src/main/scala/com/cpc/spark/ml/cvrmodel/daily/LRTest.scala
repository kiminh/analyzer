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

    // model
    model.loadLRmodel("hdfs://emr-cluster/user/cpc/lrmodel/lrmodeldata/2019-09-04-05-20")

    // generate feature vector manually.
    var els = Seq[(Int, Double)]()

    els = els :+ (3, 1d)  //week
    els = els :+ (28, 1d) // hour
    els = els :+ (31, 1d) // sex
    els = els :+ (40, 1d) // age
    els = els :+ (141, 1d) // os
    els = els :+ (150, 1d) // isp
    els = els :+ (171, 1d) // network
    els = els :+ (543, 1d) // cityid
    els = els :+ (548, 1d) // mediaid
    els = els :+ (593, 1d) // slotid
    els = els :+ (1002, 1d) // phone_level
    els = els :+ (1008, 1d) // pagenum
    els = els :+ (1108, 1d) // bookid
    els = els :+ (1208, 1d) // adclass
    els = els :+ (1301, 1d) // adtype
    els = els :+ (1309, 1d) // adslot_type
    els = els :+ (1318, 1d) // planid
    els = els :+ (10207, 1d) // unitid
    els = els :+ (20194, 1d) // ideaid

    /*els = els :+ (68621, 1d)
    els = els :+ (68652, 1d)
    els = els :+ (68612, 1d)
    els = els :+ (68611, 1d)
    els = els :+ (68644, 1d)
    els = els :+ (68618, 1d)
    els = els :+ (68616, 1d)
    els = els :+ (68769, 1d)
    els = els :+ (68655, 1d)
    els = els :+ (68642, 1d)
    els = els :+ (68617, 1d)
    els = els :+ (68614, 1d)
    els = els :+ (68695, 1d)
    els = els :+ (68645, 1d)
    els = els :+ (68632, 1d)
    els = els :+ (68613, 1d)
    els = els :+ (69583, 1d)*/

    /*var i = 0

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
    i += 16927 + 1 // ideaid*/

    //ctr
    val mediaid=44
    val planid=8888
    val unitid=9986
    val ideaid=24735
    val slotid=404
    val adclass=89
    val cityid=367

    //cvr
//    val mediaid=47
//    val planid=9548
//    val unitid=10755
//    val ideaid=26635
//    val slotid=414
//    val adclass=93
//    val cityid=367

    val size=7+24+9+100+10+20+10+cityid+1+mediaid+1+slotid+1+10+100+100+adclass+1+16+10+planid+1+unitid+1+ideaid+1
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