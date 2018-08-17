package com.cpc.spark.ml.train

import java.io.FileOutputStream
import java.util.Date

import com.cpc.spark.qukan.utils.RedisUtil
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import com.cpc.spark.qukan.utils.Udfs.udfSnapshotToLeafFeatures
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel._
import com.cpc.spark.ml.common.{Utils => MUtils}

import scala.collection.mutable

object FtrlSnapshot {

  def main(args: Array[String]): Unit = {

    val dt = args(0)
    val hour = args(1)
    val upload = args(2).toInt

    val version = 15

    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("ftrl hourly")
      .enableHiveSupport()
      .getOrCreate()


    val features = spark.sql(s"select *, raw_int['ideaid'] as ideaid from dl_cpc.ml_snapshot where `date`='$dt' and hour = '$hour'")
    println(s"features count = ${features.count()}")
    val log = spark.table("dl_cpc.cpc_union_log").filter(s"`date`='$dt' and hour = '$hour'")
      .filter("media_appsid  in ('80000001', '80000002') and isshow = 1 and ext['antispam'].int_value = 0 " +
        "and ideaid > 0 and adsrc = 1 and adslot_type in (1) AND userid > 0 "
        //        + "AND (ext['charge_type'] IS NULL OR ext['charge_type'].int_value = 1"
      )

    val featureSearchid = features.select("searchid").distinct()
    val featureSearchidCount = featureSearchid.count()
    println(s"feature searchid count = $featureSearchidCount")
    val logHasFeatureSearchId = log.join(featureSearchid, Seq("searchid"), "inner")
    val logHasFeatureSearchIdCount = logHasFeatureSearchId.select("searchid").distinct().count()
    println(s"log has feature searchid count = $logHasFeatureSearchIdCount")
    val join = log.join(features, Seq("searchid", "ideaid"), "inner")
    val joinCount = join.select("searchid").distinct().count()
    println(s"join count = $joinCount")

    val positive = join.filter("isclick=1")
    val negtive = join.filter("isclick=0").sample(false, 0.2)
    println(s"positive count = ${positive.count()}")
    println(s"negtive count = ${negtive.count()}")
    val sample = positive.union(negtive).withColumn("leaf_features", udfSnapshotToLeafFeatures(col("feature_vector")))
      .rdd.map(x => {
      val size = 500000
      val array = x.getAs[String]("leaf_features").split("\\s+")
      val label = x.getAs[Int]("isclick").toDouble
      val vector1 = array.map(x => {
        (x.toInt, 1.0)
      })
      val vec = Vectors.sparse(size, vector1)
      LabeledPoint(label, vec)
    })

    var ftrlnew = new Ftrl()
    var ftrlRedis = RedisUtil.redisToFtrl(version)
    var ftrl = if (ftrlRedis != null) {
      println("from redis")
      ftrlRedis
    } else {
      println("new")
      ftrlnew
    }
    //    val ftrl = ftrlnew
    ftrl.train(spark, sample)
    // ftrl.print()
    RedisUtil.ftrlToRedis(ftrl, version)


    // upload
    val fname = s"ctr-portrait${version}-ftrl-qtt-list.mlm"
    val filename = s"/home/cpc/djq/xgboost_lr/$fname"
    saveLrPbPack(ftrl, filename, "ftrl", version)
    println(fname, filename)

    if (upload > 0) {
      val conf = ConfigFactory.load()
      println(MUtils.updateMlcppOnlineData(filename, s"/home/work/mlcpp/data/$fname", conf))
    }

  }

  def saveLrPbPack(ftrl: Ftrl, path: String, parser: String, version: Int): Unit = {
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
      name = s"qtt-list-ctr-ftrl-portrait${version}",
      createTime = new Date().getTime,
      lr = Option(lr),
      ir = Option(ir),
      dict = Option(dictpb),
      strategy = Strategy.StrategyXgboostFtrl,
      gbmfile = s"data/ctr-portrait9-qtt-list.gbm",
      gbmTreeLimit = 200,
      gbmTreeDepth = 10,
      negSampleRatio = 0.2
    )
    pack.writeTo(new FileOutputStream(path))
  }

}
