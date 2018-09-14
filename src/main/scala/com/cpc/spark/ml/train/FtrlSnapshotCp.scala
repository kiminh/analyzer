package com.cpc.spark.ml.train

import java.io.FileOutputStream
import java.util.Date

import com.cpc.spark.common.Utils
import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.qukan.utils.RedisUtil
import com.cpc.spark.qukan.utils.Udfs.udfSnapshotToLeafFeatures
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.functions.col

object FtrlSnapshotCp {

  def main(args: Array[String]): Unit = {

    val ftrlFeatureSize = 500000

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

    val version = 800015

    val spark = Utils.buildSparkSession(name = "v800015_ctr")

    val dateRangeSql = Utils.getTimeRangeSql(startDt, startHr, dt, hour)
    val featuresSql = s"select *, raw_int['ideaid'] as ideaid from dl_cpc.ml_snapshot where $dateRangeSql"
    println("feature sql:")
    println(featuresSql)
    val features = spark.sql(featuresSql)
    val log = spark.table("dl_cpc.cpc_union_log").filter(dateRangeSql)
      .filter("media_appsid  in ('80000001', '80000002') and isshow = 1 and ext['antispam'].int_value = 0 " +
        "and ideaid > 0 and adsrc = 1 and adslot_type in (1) AND userid > 0 "
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

      val array = x.getAs[String]("leaf_features").split("\\s+")
      val label = x.getAs[Int]("isclick").toDouble
      val vector1 = array.map(x => {
        (x.toInt, 1.0)
      })
      val vec = Vectors.sparse(ftrlFeatureSize, vector1)
      LabeledPoint(label, vec)
    })

    var ftrlnew = new Ftrl(ftrlFeatureSize)
    var ftrlRedis = RedisUtil.redisToFtrl(version, ftrlFeatureSize)
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

    if (upload) {
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
