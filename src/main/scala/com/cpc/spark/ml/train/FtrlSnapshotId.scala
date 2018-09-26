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

//  val XGBOOST_FEATURE_SIZE = 500000
////  val ID_FEATURES_SIZE = 10000000
//
//  val ADVERTISER_ID_NAME = "advertiser"
//  val PLAN_ID_NAME = "plan"
//
//  val LOCAL_DIR = "/home/cpc/ftrl/v20/"
//  val DEST_DIR = "/home/work/mlcpp/model/v20/"
//
//  val DOWN_SAMPLE_RATE = 0.2
//
//  def main(args: Array[String]): Unit = {
//
//    val dt = args(0)
//    val hour = args(1)
//    val hourRange = args(2).toInt
//    val upload = args(3).toBoolean
//    val startFresh = args(4).toBoolean
//    val devMode = args(5).toBoolean
//
//    println(s"dt=$dt")
//    println(s"hour=$hour")
//    println(s"hourRange=$hourRange")
//    println(s"upload=$upload")
//    println(s"startFresh=$startFresh")
//    println(s"devMode=$devMode")
//
//    val (startDt, startHr) = Utils.getStartDateHour(dt, hour, hourRange)
//    println(s"startDate=$startDt")
//    println(s"startHour=$startHr")
//
//    val version = 20
//
//    val spark = Utils.buildSparkSession(name = "v20_ctr")
//
//    // get xgboost features
//    val dateRangeSql = Utils.getTimeRangeSql(startDt, startHr, dt, hour)
//    val featuresSql = s"select *, raw_int['ideaid'] as ideaid from dl_cpc.ml_snapshot where $dateRangeSql"
//    println("feature sql:")
//    println(featuresSql)
//    val features = spark.sql(featuresSql)
//    // get union_log
//    var log = spark.table("dl_cpc.cpc_union_log").filter(dateRangeSql)
//      .filter("media_appsid  in ('80000001', '80000002') and isshow = 1 and ext['antispam'].int_value = 0 " +
//        "and ideaid > 0 and adsrc = 1 and adslot_type in (1) AND userid > 0 ")
//
//    if (devMode) {
//      log = log.limit(1000)
//    }
//
//    val join = log.join(features, Seq("searchid", "ideaid"), "inner")
//    val joinCount = join.select("searchid").distinct().count()
//    println(s"before sample join count = $joinCount")
//
//    val positive = join.filter("isclick=1")
//    val negtive = join.filter("isclick=0").sample(false, DOWN_SAMPLE_RATE)
//    println(s"positive count = ${positive.count()}")
//    println(s"negtive count = ${negtive.count()}")
//
//    val ftrl = Ftrl.getModel(version, startFresh, "list", XGBOOST_FEATURE_SIZE + ID_FEATURES_SIZE)
//
//    println("before training model info:")
//    printModelInfo(ftrl)
//
//    // merge and add xgboost features
//    var mergedDf = positive.union(negtive).withColumn("leaf_features", udfSnapshotToLeafFeatures(col("feature_vector")))
//    // join user app
//    val userApps = spark.table("dl_cpc.cpc_user_installed_apps").filter(s"load_date='$dt'")
//    mergedDf = mergedDf.join(userApps, Seq("uid"), joinType = "left")
//    val merged = addIdFeatures(mergedDf)
//
//
//    val sample = merged.map(x => x._1)
//
//    // model training
//    ftrl.train(spark, sample)
//
//    // update id map
//    val ids = merged.map(x => x._2).flatMap(x => x).distinct().collect()
//    val stringIDs = merged.map(x => x._3).flatMap(x => x).distinct().collect()
//    updateDict(ftrl, ids, stringIDs, ID_FEATURES_SIZE, XGBOOST_FEATURE_SIZE)
//
//    println("after training model info:")
//    printModelInfo(ftrl)
//
//    println(checkCollision(ftrl))
//
//    // save model file locally
//    val name = s"ctr-qtt-list-$version"
//    val filename = s"$LOCAL_DIR$name.mlm"
//    Ftrl.saveLrPbPack(ftrl, filename, "ftrl", name, mode = ID_FEATURES_SIZE, offset = XGBOOST_FEATURE_SIZE)
//    println(s"Save model locally to $filename")
//
//    // upload to redis and mlcpp machine
//    if (upload) {
//      val ret = RedisUtil.ftrlToRedis(ftrl, version)
//      println(s"upload to redis: ${ret._1} with key: ${ret._2}")
//      println(MUtils.updateMlcppOnlineData(filename, s"$DEST_DIR$name.mlm", ConfigFactory.load()))
//    }
//  }
//
//  def printModelInfo(ftrl: Ftrl): Unit = {
//    println(s"adv map size: ${ftrl.dict.advertiserid.size}")
//    println(s"plan map size: ${ftrl.dict.planid.size}")
//    println(s"string map size: ${ftrl.dict.stringid.size}")
//  }
//
//
//
//
//
//
//
//
//
//  def getHashedID(id: Int, name: String, size: Int, offset: Int): Int = {
//    return getHashedID(name + id.toString, size, offset)
//  }
//
//  def getHashedID(id: String, size: Int, offset: Int): Int = {
//    return (Math.abs(Murmur3Hash.stringHash64(id,0) % size) + offset).toInt
//  }


}
