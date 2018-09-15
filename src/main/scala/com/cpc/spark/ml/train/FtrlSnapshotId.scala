package com.cpc.spark.ml.train

/**
  * author: huazhenhao
  * date: 9/14/18
  */

import java.util.Date

import com.cpc.spark.common.Utils
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
  val ID_FEATURES_SIZE = 1000000

  val ADVERTISER_ID_NAME = "advertiser"
  val PLAN_ID_NAME = "plan"

  val LOCAL_DIR = "/home/cpc/ftrl/v17/"
  val DEST_DIR = "/home/work/mlcpp/model/v17/"

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

    val version = 17

    val spark = Utils.buildSparkSession(name = "v17_ctr")

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

    val ftrl = getModel(version, startFresh)

    println("before training model info:")
    printModelInfo(ftrl)

    val mergedDf = positive.union(negtive).withColumn("leaf_features", udfSnapshotToLeafFeatures(col("feature_vector")))
    val merged = addIdFeatures(mergedDf)


    val sample = merged.map(x => x._1)

    // model training
    ftrl.train(spark, sample)

    // update id map
    val ids = merged.map(x => x._2).flatMap(x => x).distinct().collect()
    updateDict(ftrl, ids)

    println("after training model info:")
    printModelInfo(ftrl)

    // save model file locally
    val name = s"ctr-qtt-list-$version"
    val filename = s"$LOCAL_DIR$name.mlm"
    saveLrPbPack(ftrl, filename, "ftrl", name)
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
  }

  def addIdFeatures(df: DataFrame): RDD[(LabeledPoint, Seq[(Int, String)])] = {
    return df.rdd.map(x => {
      val array = x.getAs[String]("leaf_features").split("\\s+")
      val label = x.getAs[Int]("isclick").toDouble
      val xgboostFeatures = array.map(x => {
        (x.toInt, 1.0)
      })
      val ids = getAllIDFeatures(x)
      val sparseIDFeatures = ids.map(a => {
        val hashID = getHashedID(a._1, a._2, ID_FEATURES_SIZE, XGBOOST_FEATURE_SIZE)
        (hashID, 1.0)
      })
      val vec = Vectors.sparse(ID_FEATURES_SIZE + XGBOOST_FEATURE_SIZE, xgboostFeatures ++ sparseIDFeatures)
      (LabeledPoint(label, vec), ids)
    })
  }

  def getModel(version: Int, startFresh: Boolean): Ftrl = {
    val ftrlnew = new Ftrl(ID_FEATURES_SIZE + XGBOOST_FEATURE_SIZE)
    val ftrlRedis = RedisUtil.redisToFtrl(version, ID_FEATURES_SIZE + XGBOOST_FEATURE_SIZE)
    val ftrl = if (ftrlRedis != null && !startFresh) {
      println("ftrl fetched from redis")
      ftrlRedis
    } else {
      println("new ftrl")
      ftrlnew
    }
    return ftrl
  }

  def updateDict(ftrl: Ftrl, ids: Array[(Int, String)]): Unit = {
    val advertiserMutMap = mutable.Map[Int, Int]()
    val planMutMap = mutable.Map[Int, Int]()
    ids.foreach(x => {
      val id = x._1
      val name = x._2
      val hashedID = getHashedID(id, name, ID_FEATURES_SIZE, XGBOOST_FEATURE_SIZE)
      if (name.equals(ADVERTISER_ID_NAME)) {
        advertiserMutMap.put(id, hashedID)
      }
      if (name.equals(PLAN_ID_NAME)) {
        planMutMap.put(id, hashedID)
      }
    })
    ftrl.dict.planid.foreach( x => planMutMap.put(x._1, x._2))
    ftrl.dict.advertiserid.foreach( x => advertiserMutMap.put(x._1, x._2))

    ftrl.dict = Dict(
      advertiserid = advertiserMutMap.toMap,
      planid = planMutMap.toMap
    )
  }

  def getAllIDFeatures(row: Row): Seq[(Int, String)] = {
    val originID = new ListBuffer[(Int, String)]()
    // advertiser id
    val advertiserID = row.getAs[Int]("userid")
    originID += ((advertiserID, ADVERTISER_ID_NAME))
    // plan id
    val planID = row.getAs[Int]("planid")
    originID += ((planID, PLAN_ID_NAME))
    return originID
  }

  def getHashedID(id: Int, name: String, size: Int, offset: Int): Int = {
    return (Utils.djb2Hash(name + id.toString) % ULong(size)).toInt + offset
  }

  def saveLrPbPack(ftrl: Ftrl, path: String, parser: String, name: String): Unit = {
    val lr = LRModel(
      parser = parser,
      featureNum = ftrl.w.length,
      weights = ftrl.w.zipWithIndex.toMap.map(x => (x._2, x._1))
    )
    val ir = IRModel(
    )
    val pack = Pack(
      name = name,
      createTime = new Date().getTime,
      lr = Option(lr),
      ir = Option(ir),
      dict = Option(ftrl.dict),
      strategy = Strategy.StrategyXgboostFtrl,
      gbmfile = s"data/ctr-portrait9-qtt-list.gbm",
      gbmTreeLimit = 200,
      gbmTreeDepth = 10,
      negSampleRatio = 0.2
    )
    Utils.saveProtoToFile(pack, path)
  }

}
