package com.cpc.spark.ml.ftrl

/**
  * author: huazhenhao
  * date: 9/18/18
  */

import java.io.FileOutputStream
import java.util.Date

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ml.train.Ftrl
import com.cpc.spark.ml.train.FtrlSnapshotId.getModel
import com.cpc.spark.qukan.utils.RedisUtil
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FtrlNewHourlyID {

  val XGBOOST_FEATURE_SIZE = 500000
  val ID_FEATURES_SIZE = 10000000

  val ADVERTISER_ID_NAME = "advertiser"
  val PLAN_ID_NAME = "plan"

  val LOCAL_DIR = "/home/cpc/ftrl/"
  val DEST_DIR = "/home/work/mlcpp/model/"

  val DOWN_SAMPLE_RATE = 0.2

  def main(args: Array[String]): Unit = {

    val dt = args(0)
    val hour = args(1)
    val upload = args(3).toBoolean
    val startFresh = args(4).toBoolean
    val typename = args(5)
    val gbdtVersion = args(6).toInt
    val ftrlVersion = args(7).toInt
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

    val inputName = s"/user/cpc/qtt-portrait-ftrl/sample_for_ftrl_with_id/ftrl-${dt}-${hour}-${typename}-${gbdtVersion}.svm"
    println(s"inputname = $inputName")

    val spark = Utils.buildSparkSession(name = "full_id_ftrl")

    import spark.implicits._

    // id, label, features
    val sample = spark.sparkContext
      .textFile(inputName, 50)
      .map(x => {
        val array = x.split("\t")
        val label = array(0).toDouble
        (array(2), label, array(1))
      }).toDF("searchid", "label", "xgBoostFeatures")

    println(s"sample size = ${sample.count()}")

    val log = spark.table("dl_cpc.cpc_union_log")
      .filter(s"`date` = '$dt' and hour = '$hour'")
      .filter("media_appsid  in ('80000001', '80000002') and isshow = 1 and ext['antispam'].int_value = 0 " +
        "and ideaid > 0 and adsrc = 1 and adslot_type in (1) AND userid > 0 ")


    val join = sample.join(log, Seq("searchid"), "inner")
    println(s"before sample join count = ${join.select("searchid").distinct().count()}")

    val ftrl = getModel(ftrlVersion, startFresh)
    var ftrlnew = new Ftrl(XGBOOST_FEATURE_SIZE)
    var ftrl = if (startFresh) {
      var ftrlRedis = RedisUtil.redisToFtrlWithType(typename, ftrlVersion, XGBOOST_FEATURE_SIZE)
      if (ftrlRedis != null) {
        println("from redis")
        ftrlRedis
      } else {
        println("new")
        ftrlnew
      }
    } else {
      println("new")
      ftrlnew
    }

    // val ftrl = ftrlnew
    ftrl.train(spark, sample)
    // ftrl.print()
    RedisUtil.ftrlToRedisWithtype(ftrl, typename, ftrlVersion, dt, hour)


    // upload
    val fname = s"$ctrcvr-portrait${ftrlVersion}-ftrl-qtt-$adslot.mlm"
    val filename = s"/home/cpc/djq/xgboost_lr/$fname"
    saveLrPbPack(ftrl, filename, "ftrl", gbdtVersion, ftrlVersion, adslot, ctrcvr)
    println(fname, filename)

    if (upload) {
      val conf = ConfigFactory.load()
      println(MUtils.updateMlcppOnlineData(filename, s"/home/work/mlcpp/data/$fname", conf))
    }

  }

  def saveLrPbPack(ftrl: Ftrl, path: String, parser: String,
                   gbdtVersion: Int, ftrlVersion: Int, adslot: String, ctrcvr: String): Unit = {
    val lr = LRModel(
      parser = parser,
      featureNum = ftrl.w.length,
      weights = ftrl.w.zipWithIndex.toMap.map(x => (x._2, x._1))
    )
    val ir = IRModel(
    )
    val dictpb = Dict(

    )
    val pack = Pack(
      name = s"qtt-$adslot-$ctrcvr-ftrl-portrait${ftrlVersion}",
      createTime = new Date().getTime,
      lr = Option(lr),
      ir = Option(ir),
      dict = Option(dictpb),
      strategy = Strategy.StrategyXgboostFtrl,
      gbmfile = s"data/$ctrcvr-portrait${gbdtVersion}-qtt-$adslot.gbm",
      gbmTreeLimit = 200,
      gbmTreeDepth = 10,
      negSampleRatio = 0.2
    )
    pack.writeTo(new FileOutputStream(path))
  }

}
