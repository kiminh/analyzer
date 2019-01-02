package com.cpc.spark.ml.dnn.trash

import java.io.FileOutputStream
import java.text.SimpleDateFormat
import java.util.Date

import com.cpc.spark.ml.common.Utils
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import mlmodel.mlmodel.Pack
import org.apache.spark.mllib.regression.{IsotonicRegression, IsotonicRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by roydong on 19/03/2018.
  */
@deprecated
object PackModel {

  var spark: SparkSession = _
  private var irmodel: IsotonicRegressionModel = _
  private var irError = 0d

  def main(args: Array[String]): Unit = {

    val conf = ConfigFactory.load()
    val redis = new RedisClient(conf.getString("redis.ml_feature_ali.host"),
      conf.getInt("redis.ml_feature_ali.port"))
    redis.auth(conf.getString("redis.ml_feature_ali.auth"))
    redis.set("aa", args(5).toFloat)


    println((redis.get[Double]("aa"), redis.get[String]("aa"), redis.get[Array[Byte]]("aa")))


    println(args.mkString(" "))
    val name = args(0)       //model 名称    qtt-list-dnn-rawid
    val testfile = args(1)   //测试结果   prediction label
    val dictfile = args(2)    // dict protobuf
    val onnxfile = args(3)     //tensorflow dump to onnx
    val upload = args(4).toInt
    /*
    spark = SparkSession.builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName("test dnn model and run IR")
      .enableHiveSupport()
      .getOrCreate()
    */
    val filetime = new SimpleDateFormat("yyyy-MM-dd-HH-mm").format(new Date().getTime)

    val filename = "/home/cpc/anal/dnn_model/%s-%s".format(name, filetime)
    println(filename)

    val pack = Pack(
      strategy = mlmodel.mlmodel.Strategy.StrategyDNNRawID,
      dnnGraphFile = name + ".pb",
      dnnIdxFile = name + ".idx",
      dnnEmbedFile = name + ".embed",
      dnnEmbedWidth = 10,
      createTime = new Date().getTime
    )
    pack.writeTo(new FileOutputStream(s"$filename.mlm"))

    if (upload == 1) {
      val conf = ConfigFactory.load()
      println(Utils.updateMlcppModelData(filename+".mlm", s"/home/cpc/model_server/$name.mlm", conf))
    }
  }

  def runIr(results: RDD[(Double, Double)], binNum: Int, rate: Double): Double = {
    val sample = results.randomSplit(Array(rate, 1 - rate), seed = new Date().getTime)
    val bins = binData(sample(0), binNum)
    val sc = spark.sparkContext
    val ir = new IsotonicRegression().setIsotonic(true).run(sc.parallelize(bins.map(x => (x._1, x._3, 1d))))    // 真实值y轴，预测均值x轴   样本量就是桶的个数
    val sum = sample(1) //在测试数据上计算误差
      .map(x => (x._2, ir.predict(x._1))) //(click, calibrate ctr)
      .reduce((x, y) => (x._1 + y._1, x._2 + y._2))

    irError = (sum._2 - sum._1) / sum._1 //误差比
    irmodel = ir
    irError
  }

  private def binData(sample: RDD[(Double, Double)], binNum: Int): Seq[(Double, Double, Double, Double)] = {
    val binSize = sample.count().toInt / binNum  //每个桶的容量
    var bins = Seq[(Double, Double, Double, Double)]()
    var click = 0d  //正例数
    var pv = 0d  //总数或展示数
    var pSum = 0d  //预测值的累加
    var pMin = 1d  // 最小的预测值
    var pMax = 0d  // 最大的预测值
    var n = 0  //控制打印
    sample.sortByKey()  //(p, label)按照XGB预测值升序排序
      .toLocalIterator
      .foreach {
        x =>
          pSum = pSum + x._1
          if (x._1 < pMin) {
            pMin = x._1
          }
          if (x._1 > pMax) {
            pMax = x._1
          }
          if (x._2 > 0.01) {
            click = click + 1
          }
          pv = pv + 1
          if (pv >= binSize) {  //如果超过通的容量，就换下一个桶
            val ctr = click / pv    //  点击/展示
            bins = bins :+ (ctr, pMin, pSum / pv, pMax)  // 真实值，最小值，预测均值，最大值
            n = n + 1
            if (n > binNum - 20) {
              val logStr = "bin %d: %.6f(%d/%d) %.6f %.6f %.6f".format(
                n, ctr, click.toInt, pv.toInt, pMin, pSum / pv, pMax) //桶号：真实ctr（点击/展示），最小值，预测均值，最大值

              println(logStr)
            }

            click = 0d
            pv = 0d
            pSum = 0d
            pMin = 1d
            pMax = 0d
          }
      }
    bins
  }

}
