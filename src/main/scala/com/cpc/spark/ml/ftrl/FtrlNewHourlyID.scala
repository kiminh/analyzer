package com.cpc.spark.ml.ftrl

/**
  * author: huazhenhao
  * date: 9/18/18
  */

import com.cpc.spark.common.{Murmur3Hash, Utils}
import com.cpc.spark.ml.train.Ftrl
import com.cpc.spark.qukan.utils.RedisUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object FtrlNewHourlyID {

  val XGBOOST_FEATURE_SIZE = 500000

  val ADVERTISER_ID_NAME = "advertiser"
  val PLAN_ID_NAME = "plan"

  val DOWN_SAMPLE_RATE = 0.2

  def mapFunc(line: String): (String, Double, String, Int) = {
    val array = line.split("\t")
    if (array.length < 3) {
      return ("", 0, "", 1)
    }
    val label = array(0).toDouble
    return (array(2).trim, label, array(1), 0)
  }

  def main(args: Array[String]): Unit = {

    val dt = args(0)
    val hour = args(1)
    val upload = args(2).toBoolean
    val startFresh = args(3).toBoolean
    val typename = args(4)
    val gbdtVersion = args(5).toInt
    val ftrlVersion = args(6).toInt
    val l1 = args(7).toDouble
    val decayed = args(8).toBoolean
    val redisID = args(9).toInt
    val typearray = typename.split("-")
    val adslot = typearray(0)
    val ctrcvr = typearray(1)

    println(s"dt=$dt")
    println(s"hour=$hour")
    println(s"typename=$typename (list-ctr, content-ctr, interact-ctr, all-cvr)")
    println(s"gbdtVersion=$gbdtVersion")
    println(s"ftrlVersion=$ftrlVersion")
    println(s"l1=$l1")
    println(s"upload=$upload")
    println(s"forceNew=$startFresh")
    println(s"adslot=$adslot")
    println(s"ctrcvr=$ctrcvr")
    println(s"decayed=$decayed")
    println(s"redisID=$redisID")


    val inputName = s"/user/cpc/qtt-portrait-ftrl/sample_for_ftrl_with_id/ftrl-with-id-${dt}-${hour}-${typename}-${gbdtVersion}.svm"
    println(s"inputname = $inputName")

    val spark: SparkSession = Utils.buildSparkSession(name = "full_id_ftrl_22")

    import spark.implicits._

    // id, label, features
    val sample = spark.sparkContext
      .textFile(inputName, 50)
      .map(mapFunc)
      .toDF("searchid", "label", "xgBoostFeatures", "hasError")

    println(s"xgBoost total data size = ${sample.count()}")
    println(s"xgBoost filtered data size = ${sample.filter(x => x.getAs[Int]("hasError") > 0).count()}")
    println(s"xgBoost correct data size = ${sample.filter(x => x.getAs[Int]("hasError") == 0).count()}")

    val log = spark.table("dl_cpc.cpc_union_log")
      .filter(s"`date` = '$dt' and hour = '$hour'")
      .filter("media_appsid  in ('80000001', '80000002') and isshow = 1 and ext['antispam'].int_value = 0 " +
        "and ideaid > 0 and adsrc = 1 and adslot_type in (1) AND userid > 0")

    var merged = sample
      .filter(x => x.getAs[Int]("hasError") == 0)
      .join(log, Seq("searchid"), "inner")
    println(s"join with log size = ${merged.select("searchid").distinct().count()}")

    // join user app
    val userApps = spark.table("dl_cpc.cpc_user_installed_apps").filter(s"load_date='$dt'")
    merged = merged.join(userApps, Seq("uid"), joinType = "left")

    val ftrl = new Ftrl(1)
    val dataWithID = createFeatures(merged).collect()
    var nDict, zDict = mutable.Map[Int, Double]()
    if (!startFresh) {
      println("fetching n and z from redis...")
      val (nMap, zMap) = Ftrl.getNZFromRedis(dataWithID, redisID)
      ftrl.L1 = l1
      zDict = zMap

      if (decayed) {
        println("decay n...")
        for ((key, value) <- nMap) {
          if (value > 1.0) {
            val newValue = value * Math.exp(Math.log(0.5) / 24.0)
            nDict.put(key, newValue)
          }
        }
      } else {
        nDict = nMap
      }
    }

    println("start model training")
    val wDict = ftrl.trainWithSubDict(spark, dataWithID, nDict, zDict)

    println("after training model info:")
    println(s"batch feature size: ${wDict.size}")

    if (upload) {
      println("start saving weights to redis...")
      RedisUtil.fullModelToRedis(redisID, wDict.toMap, nDict.toMap, zDict.toMap)
    }
  }

  def printModelInfo(ftrl: Ftrl): Unit = {
    println(s"Model dict size: ${ftrl.wDict.size}")
  }

  def createFeatures(df: DataFrame): RDD[(Array[Int], Double)] = {
     df.rdd.map(x => {
      // prepare xgboost features
      val array = x.getAs[String]("xgBoostFeatures").split("\\s+")
      val xgBoostFeatures = array.map(x => {
        val vals = x.split(":")
        vals(0).toInt
      })
      // get label
      val label = x.getAs[Double]("label")
      // generate original string id
      val allId = getAllIDFeatures(x)
      // get hashed id
      val hashedID = allId.map(a => getRawHashedID(a))
      // combine xgboost feature and hashed id
      ((xgBoostFeatures.toSet ++ hashedID.toSet).toArray, label)
    })
  }

  // return: string formed id
  def getAllIDFeatures(row: Row): Seq[String] = {
    val idFeatures = new ListBuffer[String]()
    // advertiser id
    val advertiserID = row.getAs[Int]("userid")
    idFeatures.append(ADVERTISER_ID_NAME + advertiserID.toString)
    // plan id
    val planID = row.getAs[Int]("planid")
    idFeatures.append(PLAN_ID_NAME + planID.toString)

    // installed apps
    if (row.getAs[Object]("pkgs") != null) {
      val apps = row.getAs[mutable.WrappedArray[String]]("pkgs")
      apps.foreach(x => {
        idFeatures.append(x + "_installed")
        // app cross advertiser id
        idFeatures.append(x + "_installed" + advertiserID.toString + "_adv")
      })
    }
    idFeatures
  }

  def getRawHashedID(name: String): Int = {
    Murmur3Hash.stringHash64(name, 0).toInt
  }
}
