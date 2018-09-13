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

object FtrlHourly {

  def main(args: Array[String]): Unit = {

    val dt = args(0)
    val hour = args(1)
    val typename = args(2)
    val gbdtVersion = args(3).toInt
    val ftrlVersion = args(4).toInt
    val upload = args(5).toInt

    println(s"dt=$dt")
    println(s"hour=$hour")
    println(s"typename=$typename (list-ctr, content-ctr, interact-ctr, all-cvr)")
    println(s"gbdtVersion=$gbdtVersion")
    println(s"ftrlVersion=$ftrlVersion")
    println(s"upload=$upload")

    val spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryoserializer.buffer.max", "2047MB")
      .appName("ftrl hourly new ")
      .enableHiveSupport()
      .getOrCreate()


    // lr size
    var size = 500000
    val sample: RDD[LabeledPoint] = spark.sparkContext
      .textFile(s"/user/cpc/qtt-portrait-ftrl/sample_for_ftrl/ftrl-${dt}-${hour}-${typename}-${gbdtVersion}.svm", 50)
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

    var ftrlnew = new Ftrl()
    var ftrlRedis = RedisUtil.redisToFtrl(ftrlVersion)
    var ftrl = if (ftrlRedis != null) {
      println("from redis")
      ftrlRedis
    } else {
      println("new")
      ftrlnew
    }
    // val ftrl = ftrlnew
    ftrl.train(spark, sample)
    // ftrl.print()
    RedisUtil.ftrlToRedis(ftrl, ftrlVersion)


    // upload
    val fname = s"ctr-portrait${ftrlVersion}-ftrl-qtt-list.mlm"
    val filename = s"/home/cpc/djq/xgboost_lr/$fname"
    saveLrPbPack(ftrl, filename, "ftrl", gbdtVersion, ftrlVersion)
    println(fname, filename)

    if (upload > 0) {
      val conf = ConfigFactory.load()
      println(MUtils.updateMlcppOnlineData(filename, s"/home/work/mlcpp/data/$fname", conf))
    }

  }

  def saveLrPbPack(ftrl: Ftrl, path: String, parser: String, gbdtVersion: Int, ftrlVersion: Int): Unit = {
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
      name = s"qtt-list-ctr-ftrl-portrait${ftrlVersion}",
      createTime = new Date().getTime,
      lr = Option(lr),
      ir = Option(ir),
      dict = Option(dictpb),
      strategy = Strategy.StrategyXgboostFtrl,
      gbmfile = s"data/ctr-portrait${gbdtVersion}-qtt-list.gbm",
      gbmTreeLimit = 200,
      gbmTreeDepth = 10,
      negSampleRatio = 0.2
    )
    pack.writeTo(new FileOutputStream(path))
  }

}
