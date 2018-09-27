package com.cpc.spark.ml.ftrl

/**
  * author: huazhenhao
  * date: 9/18/18
  */

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ml.train.{Ftrl, FtrlSnapshotId}
import com.typesafe.config.ConfigFactory
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import com.cpc.spark.ml.train.FtrlSnapshotId._
import org.apache.spark.sql.{DataFrame, SparkSession}

object FtrlNewHourlyID {

  val XGBOOST_FEATURE_SIZE = 500000
  val ID_FEATURES_SIZE = 50000000 // 50M

  val ADVERTISER_ID_NAME = "advertiser"
  val PLAN_ID_NAME = "plan"

  val LOCAL_DIR = "/home/cpc/ftrl/"
  val HDFS_MODEL_DIR = "hdfs:///user/cpc/qtt-ftrl-model/"
  val HDFS_MODEL_HISTORY_DIR = "hdfs:///user/cpc/qtt-ftrl-model-history/"
  val DEST_DIR = "/home/work/mlcpp/model/"

  val DOWN_SAMPLE_RATE = 0.2

  // return (searchid, label, xgfeature, error)
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
    val typearray = typename.split("-")
    val adslot = typearray(0)
    val ctrcvr = typearray(1)

    println(s"dt=$dt")
    println(s"hour=$hour")
    println(s"typename=$typename (list-ctr, content-ctr, interact-ctr, all-cvr)")
    println(s"gbdtVersion=$gbdtVersion")
    println(s"ftrlVersion=$ftrlVersion")
    println(s"upload=$upload")
    println(s"forceNew=$startFresh")
    println(s"adslot=$adslot")
    println(s"ctrcvr=$ctrcvr")

    val inputName = s"/user/cpc/qtt-portrait-ftrl/sample_for_ftrl_with_id/ftrl-with-id-${dt}-${hour}-${typename}-${gbdtVersion}.svm"
    println(s"inputname = $inputName")

    val spark: SparkSession = Utils.buildSparkSession(name = "full_id_ftrl")

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

    val dataWithID = createFeatures(merged)

    val samples = dataWithID.map(x => x._1)

    val currentHDFS = s"${HDFS_MODEL_DIR}ftrl-$typename-$ftrlVersion.mlm"

    val ftrl = Ftrl.getModelFromProtoOnHDFS(startFresh, currentHDFS, spark, XGBOOST_FEATURE_SIZE + ID_FEATURES_SIZE)
    ftrl.train(spark, samples)

    val ids = dataWithID.map(x => x._2).flatMap(x => x).distinct().collect()
    val stringIDs = dataWithID.map(x => x._3).flatMap(x => x).distinct().collect()
    updateDict(ftrl, ids, stringIDs, ID_FEATURES_SIZE, XGBOOST_FEATURE_SIZE)

    println("after training model info:")
    printModelInfo(ftrl)

    println(checkCollision(ftrl))

    // save model file locally
    val name = s"$ctrcvr-protrait$ftrlVersion-ftrl-id-qtt-$adslot"
    val filename = s"$LOCAL_DIR$name.mlm"
    Ftrl.saveLrPbPack(ftrl, filename, "ctr-ftrl-v1", name, mode = ID_FEATURES_SIZE, offset = XGBOOST_FEATURE_SIZE)
    println(s"Save model locally to $filename")

    if (upload) {
      Ftrl.saveToProtoToHDFS(currentHDFS, spark, ftrl)
      val historyHDFS = s"${HDFS_MODEL_HISTORY_DIR}ftrl-$typename-$ftrlVersion-$dt-$hour.mlm"
      Ftrl.saveToProtoToHDFS(historyHDFS, spark, ftrl)
      println(MUtils.updateMlcppOnlineData(filename, s"$DEST_DIR$name.mlm", ConfigFactory.load()))
    }
  }

  def createFeatures(df: DataFrame): RDD[(LabeledPoint, Seq[(Int, String)], Seq[String])] = {
    return df.rdd.map(x => {
      // prepare xgboost features
      val array = x.getAs[String]("xgBoostFeatures").split("\\s+")
      val xgBoostFeatures = array.map(x => {
        val vals = x.split(":")
        (vals(0).toInt, 1.0)
      })
      // get label
      val label = x.getAs[Double]("label")
      // prepare id type features
      val allId = getAllIDFeatures(x)
      val ids = allId._1
      val stringIDs = allId._2
      val idSet = ids.map(a => getHashedID(a._1, a._2, ID_FEATURES_SIZE, XGBOOST_FEATURE_SIZE)).toSet
      val stringSet = stringIDs.map(a => FtrlSnapshotId.getHashedID(a, ID_FEATURES_SIZE, XGBOOST_FEATURE_SIZE)).toSet
      val sparseIDFeatures = (idSet ++ stringSet).map(a => (a, 1.0))
      val vec = Vectors.sparse(ID_FEATURES_SIZE + XGBOOST_FEATURE_SIZE, xgBoostFeatures ++ sparseIDFeatures)
      (LabeledPoint(label, vec), ids, stringIDs)
    })
  }
}
