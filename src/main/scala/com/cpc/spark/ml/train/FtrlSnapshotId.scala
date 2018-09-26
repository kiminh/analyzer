package com.cpc.spark.ml.train

/**
  * author: huazhenhao
  * date: 9/14/18
  */

import java.util.Date

import com.cpc.spark.common.{Murmur3Hash, Utils}
import com.cpc.spark.qukan.utils.RedisUtil
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.functions.col
import com.cpc.spark.qukan.utils.Udfs.udfSnapshotToLeafFeatures
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel._
import com.cpc.spark.ml.common.{Utils => MUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import spire.math.ULong

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object FtrlSnapshotId {

  val XGBOOST_FEATURE_SIZE = 500000
  val ID_FEATURES_SIZE = 10000000

  val ADVERTISER_ID_NAME = "advertiser"
  val PLAN_ID_NAME = "plan"

  val LOCAL_DIR = "/home/cpc/ftrl/v20/"
  val DEST_DIR = "/home/work/mlcpp/model/v20/"

  val DOWN_SAMPLE_RATE = 0.2

  def main(args: Array[String]): Unit = {

    val dt = args(0)
    val hour = args(1)
    val hourRange = args(2).toInt
    val upload = args(3).toBoolean
    val startFresh = args(4).toBoolean
    val devMode = args(5).toBoolean

    println(s"dt=$dt")
    println(s"hour=$hour")
    println(s"hourRange=$hourRange")
    println(s"upload=$upload")
    println(s"startFresh=$startFresh")
    println(s"devMode=$devMode")

    val (startDt, startHr) = Utils.getStartDateHour(dt, hour, hourRange)
    println(s"startDate=$startDt")
    println(s"startHour=$startHr")

    val version = 20

    val spark = Utils.buildSparkSession(name = "v20_ctr")

    // get xgboost features
    val dateRangeSql = Utils.getTimeRangeSql(startDt, startHr, dt, hour)
    val featuresSql = s"select *, raw_int['ideaid'] as ideaid from dl_cpc.ml_snapshot where $dateRangeSql"
    println("feature sql:")
    println(featuresSql)
    val features = spark.sql(featuresSql)
    // get union_log
    var log = spark.table("dl_cpc.cpc_union_log").filter(dateRangeSql)
      .filter("media_appsid  in ('80000001', '80000002') and isshow = 1 and ext['antispam'].int_value = 0 " +
        "and ideaid > 0 and adsrc = 1 and adslot_type in (1) AND userid > 0 ")

    if (devMode) {
      log = log.limit(1000)
    }

    val join = log.join(features, Seq("searchid", "ideaid"), "inner")
    val joinCount = join.select("searchid").distinct().count()
    println(s"before sample join count = $joinCount")

    val positive = join.filter("isclick=1")
    val negtive = join.filter("isclick=0").sample(false, DOWN_SAMPLE_RATE)
    println(s"positive count = ${positive.count()}")
    println(s"negtive count = ${negtive.count()}")

    val ftrl = Ftrl.getModel(version, startFresh, "list", XGBOOST_FEATURE_SIZE + ID_FEATURES_SIZE)

    println("before training model info:")
    printModelInfo(ftrl)

    // merge and add xgboost features
    var mergedDf = positive.union(negtive).withColumn("leaf_features", udfSnapshotToLeafFeatures(col("feature_vector")))
    // join user app
    val userApps = spark.table("dl_cpc.cpc_user_installed_apps").filter(s"load_date='$dt'")
    mergedDf = mergedDf.join(userApps, Seq("uid"), joinType = "left")
    val merged = addIdFeatures(mergedDf)


    val sample = merged.map(x => x._1)

    // model training
    ftrl.train(spark, sample)

    // update id map
    val ids = merged.map(x => x._2).flatMap(x => x).distinct().collect()
    val stringIDs = merged.map(x => x._3).flatMap(x => x).distinct().collect()
    updateDict(ftrl, ids, stringIDs, ID_FEATURES_SIZE, XGBOOST_FEATURE_SIZE)

    println("after training model info:")
    printModelInfo(ftrl)

    println(checkCollision(ftrl))

    // save model file locally
    val name = s"ctr-qtt-list-$version"
    val filename = s"$LOCAL_DIR$name.mlm"
    Ftrl.saveLrPbPack(ftrl, filename, "ftrl", name, mode = ID_FEATURES_SIZE, offset = XGBOOST_FEATURE_SIZE)
    println(s"Save model locally to $filename")

    // upload to redis and mlcpp machine
    if (upload) {
      val ret = RedisUtil.ftrlToRedis(ftrl, version)
      println(s"upload to redis: ${ret._1} with key: ${ret._2}")
      println(MUtils.updateMlcppOnlineData(filename, s"$DEST_DIR$name.mlm", ConfigFactory.load()))
    }
  }

  def printModelInfo(ftrl: Ftrl): Unit = {
    println(s"adv map size: ${ftrl.dict.advertiserid.size}")
    println(s"plan map size: ${ftrl.dict.planid.size}")
    println(s"string map size: ${ftrl.dict.stringid.size}")
  }

  def addIdFeatures(df: DataFrame): RDD[(LabeledPoint, Seq[(Int, String)], Seq[String])] = {
    return df.rdd.map(x => {
      val array = x.getAs[String]("leaf_features").split("\\s+")
      val label = x.getAs[Int]("isclick").toDouble
      val xgboostFeatures = array.map(x => {
        (x.toInt, 1.0)
      })
      val allId = getAllIDFeatures(x)
      val ids = allId._1
      val stringIDs = allId._2
      val idSet = ids.map(a => getHashedID(a._1, a._2, ID_FEATURES_SIZE, XGBOOST_FEATURE_SIZE)).toSet
      val stringSet = stringIDs.map(a => getHashedID(a, ID_FEATURES_SIZE, XGBOOST_FEATURE_SIZE)).toSet
      val sparseIDFeatures = (idSet ++ stringSet).map(a => (a, 1.0))
      val vec = Vectors.sparse(ID_FEATURES_SIZE + XGBOOST_FEATURE_SIZE, xgboostFeatures ++ sparseIDFeatures)
      (LabeledPoint(label, vec), ids, stringIDs)
    })
  }

  def updateDict(ftrl: Ftrl, ids: Array[(Int, String)], stringIDs: Array[String], idFeatureSize: Int, xgFeatureSize: Int): Unit = {
    val advertiserMutMap = mutable.Map[Int, Int]()
    val planMutMap = mutable.Map[Int, Int]()
    val stringMap = mutable.Map[String, Int]()
    ids.foreach(x => {
      val id = x._1
      val name = x._2
      val hashedID = getHashedID(id, name, idFeatureSize, xgFeatureSize)
      if (name.equals(ADVERTISER_ID_NAME)) {
        advertiserMutMap.put(id, hashedID)
      }
      if (name.equals(PLAN_ID_NAME)) {
        planMutMap.put(id, hashedID)
      }
    })
    stringIDs.foreach(x => {
      stringMap.put(x, getHashedID(x, idFeatureSize, xgFeatureSize))
    })
    ftrl.dict.planid.foreach( x => planMutMap.put(x._1, x._2))
    ftrl.dict.advertiserid.foreach( x => advertiserMutMap.put(x._1, x._2))
    ftrl.dict.stringid.foreach(x => stringMap.put(x._1, x._2))

    ftrl.dict = Dict(
      advertiserid = advertiserMutMap.toMap,
      planid = planMutMap.toMap,
      stringid = stringMap.toMap
    )
  }

  def checkCollision(ftrl: Ftrl): String = {
    var total = 0
    var collision = 0
    val idSet = scala.collection.mutable.Set[Int]()
    ftrl.dict.planid.foreach( x => {
      total += 1
      val k = x._2
      if (idSet.contains(k)) {
        collision += 1
      } else {
        idSet.add(x._2)
      }
    })
    ftrl.dict.advertiserid.foreach( x => {
      total += 1
      val k = x._2
      if (idSet.contains(k)) {
        collision += 1
      } else {
        idSet.add(x._2)
      }
    })
    ftrl.dict.stringid.foreach( x => {
      total += 1
      val k = x._2
      if (idSet.contains(k)) {
        collision += 1
      } else {
        idSet.add(x._2)
      }
    })
    return s"dict collision rate $collision / $total (${collision.toDouble / total}"
  }

  // return: (<typed id>, <notyped string>
  def getAllIDFeatures(row: Row): (Seq[(Int, String)], Seq[String]) = {
    val originID = new ListBuffer[(Int, String)]()
    // advertiser id
    val advertiserID = row.getAs[Int]("userid")
    originID.append((advertiserID, ADVERTISER_ID_NAME))
    // plan id
    val planID = row.getAs[Int]("planid")
    originID.append((planID, PLAN_ID_NAME))

    val stringID = new ListBuffer[String]()
    // installed apps
    if (row.getAs[Object]("pkgs") != null) {
      val apps = row.getAs[mutable.WrappedArray[String]]("pkgs")
      apps.foreach( x => {
        stringID.append(x + "_installed")
        // app cross advertiser id
        stringID.append(x + "_installed" + advertiserID.toString + "_adv")
      })
    }


    return (originID, stringID)
  }

  def getHashedID(id: Int, name: String, size: Int, offset: Int): Int = {
    return getHashedID(name + id.toString, size, offset)
  }

  def getHashedID(id: String, size: Int, offset: Int): Int = {
    return (Math.abs(Murmur3Hash.stringHash64(id, 0) % size) + offset).toInt
  }



}
