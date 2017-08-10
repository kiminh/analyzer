package com.cpc.spark.ml.sex.v2

import scala.reflect.runtime.universe
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.mllib.optimization.{L1Updater, LBFGS, LogisticGradient, SquaredL2Updater}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.Row
import java.io.File
import org.apache.commons.lang3.StringUtils
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.apache.commons.lang3.math.NumberUtils
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import com.hankcs.hanlp.seg.Segment
import com.hankcs.hanlp.HanLP
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import scala.util.hashing.MurmurHash3
import scala.util.control._
import org.apache.spark.mllib.classification.LogisticRegressionModel
import com.redis.RedisClient
import com.redis.serialization.Parse.Implicits._
import com.typesafe.config.ConfigFactory
import userprofile.Userprofile.UserProfile

/**
  * 训练性别预测模型
  */
object TrainSex{

  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    Logger.getRootLogger.setLevel(Level.WARN)
    val input = args(0).toString()
    val modelin = args(1).toString()
    val output = args(2).toString()
    val ctx = SparkSession.builder()
      .appName("TrainSexModel-NEW2")
      .enableHiveSupport()
      .getOrCreate()
    import ctx.implicits._

    val conf = ConfigFactory.load()
    val HASHSUM = 100000
    val tData = ctx.sparkContext.textFile(input).map {
      x =>
        val arr = x.split("\t")
        if (arr.length != 2) {
          var els = Seq[(Int,Double)]()
          ("-1", Vectors.sparse(HASHSUM, els))
        } else {
          val devid : String = arr(0)
          val title : String = arr(1)
          val terms = HanLP.segment(title).filter { x => x.length() > 1 }.map { x => x.word }.mkString(" ")
          val termarr = terms.split(" ")
          var done : Boolean = false
          var els = Seq[(Int,Double)]()
          for (i <- 0 to  termarr.length - 1) {
            val tag = (MurmurHash3.stringHash(termarr(i))%HASHSUM + HASHSUM)%HASHSUM
            if ( els.exists(x => (x._1 == tag) ) == false) {
              els = els :+ ( tag, 1D )
            } else
              done = false
            for (i <- 1 to 4) {
              if (els.exists(x => (x._1 == tag + HASHSUM * i)) == false && done == false) {
                els = els :+ (tag + HASHSUM*i , 1D)
                done = true
              }
            }
          }
          (devid,Vectors.sparse(HASHSUM * 5, els))
        }
    }.filter{
      x =>
        if (x._1 == "-1") {
          false
        } else {
          true
        }
    }
    val lrmodel = LogisticRegressionModel.load(ctx.sparkContext, modelin)
    lrmodel.clearThreshold()

    val ansret = tData.map {
      x =>
        val predata = lrmodel.predict(x._2)
        if (predata > 0.6 ) {
          (x._1, 1)
        } else if (predata < 0.4) {
          (x._1, 2)
        } else {
          (x._1, 0)
        }
    }
    val sum = ansret.mapPartitions {
      part =>
        var n1 = 0
        var n2 = 0
        val redis = new RedisClient(conf.getString("redis.host"), conf.getInt("redis.port"))
        part.foreach {
            x =>
              var deviceid: String = x._1
              var user: UserProfile.Builder = null
              val key = deviceid + "_UPDATA"
              val buffer = redis.get[Array[Byte]](key).getOrElse(null)
              if (buffer == null) {
                user = UserProfile.newBuilder().setDevid(deviceid)
              } else {
                user = UserProfile.parseFrom(buffer).toBuilder
                n2 = n2 + 1
              }
              user.setSex(x._2)
            //  redis.setex(key, 3600 * 24 * 7, user.build().toByteArray)
        }
        Seq((0, n1), (1, n2)).iterator
    }

    //统计新增数据
    var n1 = 0
    var n2 = 0
    sum.reduceByKey((x, y) => x + y)
      .take(3)
      .foreach {
        x =>
          if (x._1 == 0) {
            n1 = x._2
          } else {
            n2 = x._2
          }
      }
    println("total: %d updated: %d".format(n1, n2))

    val testpredictionAndLabel = ansret.map {
      x =>
        (x._2, 1)
    }.reduceByKey {
      (x,y) => x+y
    }.saveAsTextFile(output)

    println("it takes " + (System.currentTimeMillis() - startTime) / 1000 + "s")

  }
}