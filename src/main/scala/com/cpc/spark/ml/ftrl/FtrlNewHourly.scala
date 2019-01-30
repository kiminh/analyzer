package com.cpc.spark.ml.ftrl

import java.io.FileOutputStream
import java.util.Date

import com.cpc.spark.ml.common.{Utils => MUtils}
import com.cpc.spark.ml.train.Ftrl
import com.cpc.spark.qukan.utils.RedisUtil
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel._
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FtrlNewHourly {

  def main(args: Array[String]): Unit = {

    val dt = args(0)
    val hour = args(1)
    val typename = args(2)
    val gbdtVersion = args(3).toInt
    val ftrlVersion = args(4).toInt
    val upload = args(5).toInt
    val forceNew = args(6).toInt
    val typearray = typename.split("-")
    val adslot = typearray(0)
    val ctrcvr = typearray(1)

    println(s"dt=$dt")
    println(s"hour=$hour")
    println(s"typename=$typename (list-ctr, content-ctr, interact-ctr, all-cvr)")
    println(s"gbdtVersion=$gbdtVersion")
    println(s"ftrlVersion=$ftrlVersion")
    println(s"upload=$upload")
    println(s"forceNew=$forceNew")
    println(s"adslot=$adslot")
    println(s"ctrcvr=$ctrcvr")

    var inputname = s"/user/cpc/qtt-portrait-ftrl/sample_for_ftrl/ftrl-${dt}-${hour}-${typename}-${gbdtVersion}.svm"
    println(s"inputname = $inputname")

    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .config("spark.driver.maxResultSize", "10g")
      .appName("ftrl hourly new ")
      .enableHiveSupport()
      .getOrCreate()


    // lr size
    var size = 500000
    val sample: RDD[LabeledPoint] = spark.sparkContext
      .textFile(inputname, 50)
      .map { x => {
        val array = x.split("\t")
        val label = array(0).toDouble
        val vector1 = array(1).split("\\s+").map(x => {
          val array = x.split(":")
          (array(0).toInt, array(1).toDouble)
        })
        val vec = Vectors.sparse(size, vector1)
        LabeledPoint(label, vec)
      }
      }
    println(s"sample size = ${sample.count()}")

    var ftrlnew = new Ftrl(size)
    var ftrl = if (forceNew == 0) {
      var ftrlRedis = RedisUtil.redisToFtrlWithType(typename, ftrlVersion, size)
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
    RedisUtil.ftrlToRedisWithType(ftrl, typename, ftrlVersion, dt, hour)
    RedisUtil.ftrlToRedisWithTypeV2(ftrl, typename, ftrlVersion, dt, hour)


    // upload
    val fname = s"$ctrcvr-portrait${ftrlVersion}-ftrl-qtt-$adslot.mlm"
    val filename = s"/home/cpc/djq/xgboost_lr/$fname"
    saveLrPbPack(ftrl, filename, "ftrl", gbdtVersion, ftrlVersion, adslot, ctrcvr)
    println(fname, filename)

    if (upload > 0) {
      val conf = ConfigFactory.load()
      println(MUtils.updateMlcppOnlineData(filename, s"/home/work/mlcpp/data/$fname", conf))
      println(MUtils.updateMlcppModelData(filename, s"/home/cpc/model_server/data/$fname", conf))
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
